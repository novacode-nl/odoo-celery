# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html)

import logging
import os
import uuid
import xmlrpclib

from odoo import api, fields, models, registry, _
from odoo.addons.base_sparse_field.models.fields import Serialized
from odoo.exceptions import UserError
from odoo.tools import config

from ..odoo import call_task

logger = logging.getLogger(__name__)

TASK_NOT_FOUND = 'NOT_FOUND'

STATE_PENDING = 'PENDING'
STATE_STARTED = 'STARTED'
STATE_RETRY = 'RETRY'
STATE_FAILURE = 'FAILURE'
STATE_SUCCESS = 'SUCCESS'

STATES = [(STATE_PENDING, 'Pending'),
          (STATE_STARTED, 'Started'),
          (STATE_RETRY, 'Retry'),
          (STATE_FAILURE, 'Failure'),
          (STATE_SUCCESS, 'Success')]


class CeleryTask(models.Model):
    _name = 'celery.task'
    _description = 'Celery Task'
    _inherit = ['mail.thread']
    _rec_name = 'uuid'
    _order = 'create_date DESC'

    uuid = fields.Char(string='UUID', readonly=True, index=True, required=True)
    user_id = fields.Many2one('res.users', string='User ID', required=True, readonly=True)
    company_id = fields.Many2one('res.company', string='Company', index=True, readonly=True)
    model_name = fields.Char(string='Model', readonly=True)
    method_name = fields.Char(string='Task', readonly=True)
    record_ids = fields.Serialized(readonly=True)
    kwargs = fields.Serialized(readonly=True)
    started_date = fields.Datetime(string='Start Time', readonly=True)
    success_date = fields.Datetime(string='Success Time', readonly=True)
    failure_date = fields.Datetime(string='Failure Time', readonly=True)
    result = fields.Text(string='Result', readonly=True)
    state = fields.Selection(
        STATES,
        string="State",
        default=STATE_PENDING,
        required=True,
        readonly=True,
        index=True,
        help="""\
        - PENDING: The task is waiting for execution.
        - STARTED: The task has been started.
        - RETRY: The task is to be retried, possibly because of failure.
        - FAILURE: The task raised an exception, or has exceeded the retry limit.
        - SUCCESS: The task executed successfully.""")

    def call_task(self, _model_name, _method_name, _record_ids=None, **kwargs):
        """ Call Task dispatch to the Celery interface. """
        user = (os.environ.get('ODOO_CELERY_USER') or
                config.misc.get("celery", {}).get('user'))
        password = (os.environ.get('ODOO_CELERY_PASSWORD') or
                    config.misc.get("celery", {}).get('password'))

        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)
        if not user_id:
            msg = _('The user "%s" doesn\'t exist.') % user
            logger.error(msg)
            return False
        
        user_id = user_id[0]['id']

        with registry(self._cr.dbname).cursor() as cr:
            env = api.Environment(cr, user_id, {})
            try:
                res = self.with_env(env).create({
                    'uuid': str(uuid.uuid4()),
                    'user_id': user_id,
                    'model_name': _model_name,
                    'method_name': _method_name,
                    'record_ids': _record_ids,
                    'kwargs': kwargs})
                # cr.commit() # Needed?
                url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
                call_task.apply_async(args=[url, self._cr.dbname, user_id, password, res.uuid, _model_name, _method_name], kwargs=kwargs)
            except Exception as e:
                logger.error(_('ERROR IN call_task %s: %s') % (res.uuid, e))
                cr.rollback()
    @api.model
    def run_task(self, task_uuid, _model_name, _method_name, *args, **kwargs):
        """ Run/execute the task, which is a model method.

        The idea is that Celery calls this by Odoo its external API,
        whereas XML-RPC or a HTTP-controller.

        The model method is called as administator (user), to
        circumvent model-access configuration for models which
        run/process task.  Maybe change this in the futue by a
        parameter/setting? """
        
        exist = self.search_count([('uuid', '=', task_uuid)])
        if exist == 0:
            msg = "Task doesn't exist (anymore). Task-UUID: %s" % task_uuid
            logger.error(msg)
            return (TASK_NOT_FOUND, msg)
        
        model = self.env[_model_name].sudo()
        task = self.search([('uuid', '=', task_uuid)], limit=1)

        # TODO
        # Re-raise Exception if not called by XML-RPC, but directly from model/Odoo.
        # This supports unit-tests and scripting purposes.
        vals = {}
        try:
            vals.update({'state': STATE_STARTED, 'started_date': fields.Datetime.now()})
            result = getattr(model, _method_name)(**kwargs)
            vals.update({'state': STATE_SUCCESS, 'success_date': fields.Datetime.now()})
        except Exception as e:
            """ The Exception-handler does a rollback. So we need a new
            transaction/cursor to store data about Failure. """
            # TODO
            # - Could STATE_RETRY be set here?
            # Possibile retry(s) could be registered somewhere, e.g. in the task/model object?
            # - Add (exc)trace to task record.
            result = "%s: %s" % (type(e).__name__, e.message)
            vals.update({'state': STATE_FAILURE, 'failure_date': fields.Datetime.now(), 'result': result})
        finally:
            with registry(self._cr.dbname).cursor() as cr:
                env = api.Environment(cr, self._uid, {})
                task.with_env(env).write(vals)
            return (vals.get('state'), result)
