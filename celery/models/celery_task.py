# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html)

import logging
import os
import uuid

from odoo import api, fields, models, _
from odoo.exceptions import UserError
from odoo.tools import config

from ..celery_tasks import call_task

_logger = logging.getLogger(__name__)


class CeleryTask(models.Model):
    _name = 'celery.task'
    _description = 'Celery Task'
    _inherit = ['mail.thread']
    _order = 'create_date DESC'

    uuid = fields.Char(string='UUID', readonly=True, index=True, required=True)
    user_id = fields.Many2one('res.users', string='User ID', required=True, readonly=True)
    company_id = fields.Many2one('res.company', string='Company', index=True, readonly=True)
    model_name = fields.Char(string='Model', readonly=True)
    method_name = fields.Char(string='Task', readonly=True)
    record_ids = fields.Serialized(readonly=True)
    kwargs = fields.Serialized(readonly=True)

    def call_task(self, model_name, method_name, record_ids=None, **kwargs):
        user = (os.environ.get('ODOO_CELERY_USER') or
                config.misc.get("celery", {}).get('user'))
        password = (os.environ.get('ODOO_CELERY_PASSWORD') or
                    config.misc.get("celery", {}).get('password'))

        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)
        if not user_id:
            msg = _('The user "%s" doesn\'t exist.') % user
            _logger.error(msg)
            # raise UserError(msg)
            return
        
        user_id = user_id[0]['id']
        res = self.create({
            'uuid': str(uuid.uuid4()),
            'user_id': user_id,
            'model_name': model_name,
            'method_name': method_name,
            'record_ids': record_ids,
            'kwargs': kwargs
        })
        url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
        # TODO:
        # 1. Hiding sensitive information in arguments
        #    http://docs.celeryproject.org/en/latest/userguide/tasks.html#hiding-sensitive-information-in-arguments
        call_task.apply_async(args=[url, self._cr.dbname, user_id, password, res.uuid, model_name, method_name], **kwargs)

    @api.model
    def run_task(self, task_uuid, model_name, method_name, *args, **kwargs):
        # Run task as administator (to prevent loads of access configuration)
        model = self.env[model_name].sudo()
        res = getattr(model, method_name)(**kwargs)
        return res
