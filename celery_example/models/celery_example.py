# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import logging

from odoo import api, fields, models, _

_logger = logging.getLogger(__name__)


class CeleryExample(models.Model):
    _name = 'celery.example'
    _description = 'Celery Example'

    name = fields.Char(default='Celery Example')

    @api.multi
    def action_schedule_log_message(self):
        self.env["celery.task"].call_task("celery.example", "schedule_log_message")

    @api.model
    def schedule_log_message(self, task_uuid, **kwargs):
        msg = 'CELERY called task: model [%s] and method [schedule_log_message].' % self._name
        _logger.info(msg)
        return msg
