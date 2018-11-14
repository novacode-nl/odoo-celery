# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import logging

from odoo import api, fields, models, _

_logger = logging.getLogger(__name__)


class CeleryExample(models.Model):
    _name = 'celery.example'
    _description = 'Celery Example'

    name = fields.Char(default='Celery Example', required=True)
    lines = fields.One2many('celery.example.line', 'example_id', string='Lines')

    @api.multi
    def action_queue_example_line(self):
        self.env["celery.task"].call_task("celery.example", "queue_example_line", example_id=self.id)

    @api.multi
    def action_queue_countdown_example_line(self):
        # Several Celery params can be set by the celery kwarg.
        celery = {'countdown': 10}
        self.env["celery.task"].call_task("celery.example", "queue_countdown_example_line", example_id=self.id, celery=celery)

    @api.model
    def queue_example_line(self, task_uuid, **kwargs):
        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'name': 'Created by queued task',
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [queue_example_line].' % self._name
        _logger.info(msg)
        return msg

    @api.model
    def queue_countdown_example_line(self, task_uuid, **kwargs):
        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'name': 'Created by queued task with countdown (10 sec)',
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [queue_countdown_example_line].' % self._name
        _logger.info(msg)
        return msg

    @api.multi
    def refresh_view(self):
        return True


class CeleryExampleLine(models.Model):
    _name = 'celery.example.line'
    _description = 'Celery Example Line'

    name = fields.Char(required=True)
    example_id = fields.Many2one('celery.example', string='Example', required=True, ondelete='cascade')
