# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import logging
import time

from odoo import api, fields, models, _

_logger = logging.getLogger(__name__)


class CeleryExample(models.Model):
    _name = 'celery.example'
    _description = 'Celery Example'

    name = fields.Char(default='Celery Example', required=True)
    lines = fields.One2many('celery.example.line', 'example_id', string='Lines')

    @api.multi
    def action_task_queue_default(self):
        celery = {
            'countdown': 3,
            'retry': True,
            'max_retries': 2,
            'interval_start': 2
        }
        self.env["celery.task"].call_task("celery.example", "task_queue_default", example_id=self.id, celery=celery)

    @api.multi
    def action_task_queue_high(self):
        celery = {'queue': 'high', 'countdown': 2}
        self.env["celery.task"].call_task("celery.example", "task_queue_high", example_id=self.id, celery=celery)

    @api.multi
    def action_task_queue_low(self):
        celery = {'queue': 'low', 'countdown': 10}
        self.env["celery.task"].call_task("celery.example", "task_queue_low", example_id=self.id, celery=celery)

    @api.model
    def task_queue_default(self, task_uuid, **kwargs):
        task = 'task_queue_default'
        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'namexx': task,
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [%s].' % (self._name, task)
        _logger.info(msg)
        return msg

    @api.model
    def task_queue_high(self, task_uuid, **kwargs):
        time.sleep(10)
        task = 'task_queue_high'
        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'name': task,
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [%s].' % (self._name, task)
        _logger.info(msg)
        return msg

    @api.model
    def task_queue_low(self, task_uuid, **kwargs):
        time.sleep(5)

        task = 'task_queue_low'
        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'name': task,
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [%s].' % (self._name, task)
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
