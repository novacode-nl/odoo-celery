# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import logging
import os
import sys
import traceback
import uuid

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

def _get_celery_user_config():
    user = (os.environ.get('ODOO_CELERY_USER') or config.misc.get("celery", {}).get('user'))
    password = (os.environ.get('ODOO_CELERY_PASSWORD') or config.misc.get("celery", {}).get('password'))
    sudo = (os.environ.get('ODOO_CELERY_SUDO') or config.misc.get("celery", {}).get('sudo'))
    return (user, password, sudo)


class CeleryTask(models.Model):
    _name = 'celery.task'
    _description = 'Celery Task'
    # TODO Configure "Celery" group to access mail_thread ?
    #_inherit = ['mail.thread']
    _rec_name = 'uuid'
    _order = 'create_date DESC'

    uuid = fields.Char(string='UUID', readonly=True, index=True, required=True)
    user_id = fields.Many2one('res.users', string='User ID', required=True, readonly=True)
    company_id = fields.Many2one('res.company', string='Company', index=True, readonly=True)
    model_name = fields.Char(string='Model', readonly=True)
    method_name = fields.Char(string='Task', readonly=True)
    kwargs = fields.Serialized(readonly=True)
    started_date = fields.Datetime(string='Start Time', readonly=True)
    state_date = fields.Datetime(string='State Time', readonly=True)
    result = fields.Text(string='Result', readonly=True)
    exc_info = fields.Text(string='Exception Info', readonly=True)
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
    res_model = fields.Char(string='Related Model', readonly=True)
    res_ids = fields.Serialized(string='Related Ids', readonly=True)

    def call_task(self, _model_name, _method_name, **kwargs):
        """ Call Task dispatch to the Celery interface. """

        user, password, sudo = _get_celery_user_config()
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
                    'kwargs': kwargs})
                self._celery_call_task(user_id, password, res.uuid, _model_name, _method_name, **kwargs)
            except CeleryCallTaskException as e:
                logger.error(_('ERROR FROM call_task %s: %s') % (res.uuid, e))
                cr.rollback()
            except Exception as e:
                logger.error(_('ERROR FROM call_task: %s') % (e))
                cr.rollback()

    @api.model
    def _celery_call_task(self, user_id, password, uuid, _model_name, _method_name, **kwargs):
        try:
            url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
            call_task.apply_async(args=[url, self._cr.dbname, user_id, password, uuid, _model_name, _method_name], kwargs=kwargs)
        except Exception as e:
            raise CeleryCallTaskException

    @api.model
    def run_task(self, task_uuid, _model_name, _method_name, *args, **kwargs):
        """Run/execute the task, which is a model method.

        The idea is that Celery calls this by Odoo its external API,
        whereas XML-RPC or a HTTP-controller.

        The model-method can either be called as user:
        - The "celery" (Odoo user) defined in the odoo.conf. This is the default, in case
        the "sudo" setting isn't configured in the odoo.conf.
        - "admin" (Odoo admin user), to circumvent model-access configuration for models
        which run/process task. Therefor add "sudo = True" in the odoo.conf (see: example.odoo.conf).
        """
        
        exist = self.search_count([('uuid', '=', task_uuid)])
        if exist == 0:
            msg = "Task doesn't exist (anymore). Task-UUID: %s" % task_uuid
            logger.error(msg)
            return (TASK_NOT_FOUND, msg)

        model = self.env[_model_name]
        task = self.search([('uuid', '=', task_uuid)], limit=1)
        user, password, sudo = _get_celery_user_config()

        # TODO
        # Re-raise Exception if not called by XML-RPC, but directly from model/Odoo.
        # This supports unit-tests and scripting purposes.
        vals = {}
        result = False
        with registry(self._cr.dbname).cursor() as cr:
            # Transaction/cursror for the exception handler.
            env = api.Environment(cr, self._uid, {})
            try:
                vals.update({'state': STATE_STARTED, 'started_date': fields.Datetime.now()})
                
                if bool(sudo) and sudo:
                    res = getattr(model.with_env(env).sudo(), _method_name)(task_uuid, **kwargs)
                else:
                    res = getattr(model.with_env(env), _method_name)(task_uuid, **kwargs)
                
                if isinstance(res, dict):
                    result = res.get('result', True)
                    vals.update({'result': result, 'res_model': res.get('res_model'), 'res_ids': res.get('res_ids')})
                else:
                    result = res
                
                vals.update({'state': STATE_SUCCESS, 'state_date': fields.Datetime.now(), 'result': result})
            except Exception as e:
                """ The Exception-handler does a rollback. So we need a new
                transaction/cursor to store data about Failure. """
                # TODO
                # - Could STATE_RETRY be set here?
                # Possibile retry(s) could be registered somewhere, e.g. in the task/model object?
                # - Add (exc)trace to task record.
                exc_info = traceback.format_exc()
                vals.update({
                    'state': STATE_FAILURE,
                    'state_date': fields.Datetime.now(),
                    'exc_info': exc_info})
                logger.error('ERROR FROM run_task {uuid}: {exc_info}'.format(uuid=task_uuid, exc_info=exc_info))
                cr.rollback()
            finally:
                with registry(self._cr.dbname).cursor() as result_cr:
                    env = api.Environment(result_cr, self._uid, {})
                    task.with_env(env).write(vals)
                return (vals.get('state'), result)

    def set_state_pending(self):
        self.state = STATE_PENDING
        self.started_date = None
        self.state_date = None
        self.result = None

    @api.multi
    def requeue(self):
        user, password, sudo = _get_celery_user_config()
        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)
        user_id = user_id[0]['id']

        for task in self:
            task.set_state_pending()
            try:
                self._celery_call_task(user_id, password, task.uuid, task.model_name, task.method_name, kwargs=task.kwargs)
            except CeleryCallTaskException as e:
                logger.error(_('ERROR IN requeue %s: %s') % (task.uuid, e))
        return True

    @api.multi
    def action_open_related_record(self):
        """ Open a view with the record(s) of the task.  If it's one record,
        it opens a form-view.  If it concerns mutltiple records, it opens
        a tree view.
        """

        self.ensure_one()
        model_name = self.res_model
        records = self.env[model_name].browse(self.res_ids).exists()
        if not records:
            return None
        action = {
            'name': _('Related Record'),
            'type': 'ir.actions.act_window',
            'view_type': 'form',
            'view_mode': 'form',
            'res_model': records._name,
        }
        if len(records) == 1:
            action['res_id'] = records.id
        else:
            action.update({
                'name': _('Related Records'),
                'view_mode': 'tree,form',
                'domain': [('id', 'in', records.ids)],
            })
        return action


class RequeueTask(models.TransientModel):
    _name = 'celery.requeue.task'
    _description = 'Celery Requeue Tasks Wizard'

    @api.model
    def _default_task_ids(self):
        res = False
        context = self.env.context
        if (context.get('active_model') == 'celery.task' and
                context.get('active_ids')):
            res = context['active_ids']
        return res

    task_ids = fields.Many2many('celery.task', string='Tasks', default=_default_task_ids)

    @api.multi
    def requeue(self):
        self.task_ids.requeue()
        return {'type': 'ir.actions.act_window_close'}


class CeleryCallTaskException(Exception):
    """ CeleryCallTaskException """
