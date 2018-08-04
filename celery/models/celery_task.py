# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html)

import logging
import os
import uuid
import xmlrpclib

from openerp import api, fields, models, _
from openerp.modules.registry import RegistryManager
from openerp.tools import config

from ..odoo import call_task

logger = logging.getLogger(__name__)

TASK_SUCCESS = 'SUCCESS'
TASK_ERROR = 'ERROR'
TASK_NOT_FOUND = 'NOT_FOUND'


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

    def call_task(self, _model_name, _method_name, _record_ids=None, **kwargs):
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

        with RegistryManager.get(self._cr.dbname).cursor() as cr:
            env = api.Environment(cr, user_id, {})
            try:
                res = self.with_env(env).create({
                    'uuid': str(uuid.uuid4()),
                    'user_id': user_id,
                    'model_name': _model_name,
                    'method_name': _method_name,
                    'record_ids': _record_ids,
                    'kwargs': kwargs
                })
                cr.commit()
                url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
                call_task.apply_async(args=[url, self._cr.dbname, user_id, password, res.uuid, _model_name, _method_name], kwargs=kwargs)
            except Exception as e:
                logger.error(_('ERROR IN call_task %s: %s') % (res.uuid, e))
                cr.rollback()
    @api.model
    def run_task(self, task_uuid, _model_name, _method_name, *args, **kwargs):
        # Run task as administator (to circumvent model-access configuration for models which run/process task).
        # Maybe change this in the futue by a parameter/setting?
        exist = self.search_count([('uuid', '=', task_uuid)])
        if exist == 0:
            msg = "Task doesn't exist (anymore). Task-UUID: %s" % task_uuid
            logger.error(msg)
            return (TASK_NOT_FOUND, msg)
        model = self.env[_model_name].sudo()

        # TODO re-raise Exception if not called by XML-RPC, but directly from model/Odoo.
        # This supports unit-tests and scripting purposes.
        try:
            res = getattr(model, _method_name)(**kwargs)
            return (TASK_SUCCESS, res)
        except Exception as e:
            msg = "%s(%s)" % (type(e).__name__, e.message)
            return (TASK_ERROR, msg)
