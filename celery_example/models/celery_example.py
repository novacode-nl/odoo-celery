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

    @api.model
    def schedule_import(self, task_uuid, **kwargs):
        _logger.info('CELERY called task: model [%s] and method [schedule_import].' % self._name)

    @api.multi
    def action_schedule_import(self):
        self.env["celery.task"].call_task("celery.example", "schedule_import")
