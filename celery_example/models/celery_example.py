# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import logging
import time

from odoo import api, fields, models, _
from odoo.addons.celery.models.celery_task import RETRY_COUNTDOWN_MULTIPLY_RETRIES

_logger = logging.getLogger(__name__)


class CeleryExample(models.Model):
    _name = 'celery.example'
    _description = 'Celery Example'

    name = fields.Char(default='Celery Example', required=True)
    lines = fields.One2many('celery.example.line', 'example_id', string='Lines')

    def action_task_with_reference(self):
        celery = {
            'countdown': 10, 'retry': True,
            'retry_policy': {'max_retries': 2, 'interval_start': 2}
        }
        celery_task_vals = {
            'ref': 'celery.example.task_with_reference'
        }
        self.env["celery.task"].call_task("celery.example", "task_with_reference", example_id=self.id, celery_task_vals=celery_task_vals, celery=celery)

    def action_task_immediate(self):
        celery = {
            'countdown': 10, 'retry': True,
            'retry_policy': {'max_retries': 2, 'interval_start': 2}
        }
        celery_task_vals = {
            'ref': 'celery.example.task_immediate'
        }
        self.env["celery.task"].call_task(
            "celery.example", "task_immediate",
            example_id=self.id,
            celery_task_vals=celery_task_vals,
            celery=celery,
            transaction_strategy='immediate')

    def action_task_with_error(self):
        celery = {
            'countdown': 2,
            'retry': True,
            'max_retries': 4,
            'retry_countdown_setting': 'MUL_RETRIES_SECS',
            'retry_countdown_multiply_retries_seconds': 5,
            'retry_policy': {'interval_start': 2}
        }
        celery_task_vals = {
            'ref': 'celery.example.task_with_error'
        }
        self.env["celery.task"].call_task("celery.example", "task_with_error", example_id=self.id, celery=celery)

    def action_task_queue_default(self):
        celery = {
            'countdown': 3, 'retry': True,
            'retry_policy': {'max_retries': 2, 'interval_start': 2}
        }
        self.env["celery.task"].call_task("celery.example", "task_queue_default", example_id=self.id, celery=celery)

    def action_task_queue_high(self):
        celery = {
            'queue': 'high.priority', 'countdown': 2, 'retry': True,
            'retry_policy': {'max_retries': 2, 'interval_start': 2}
        }
        self.env["celery.task"].call_task("celery.example", "task_queue_high", example_id=self.id, celery=celery)

    def action_task_queue_low(self):
        celery = {
            'queue': 'low.priority', 'countdown': 2, 'retry': True,
            'retry_policy': {'max_retries': 2, 'interval_start': 2}
        }
        self.env["celery.task"].call_task("celery.example", "task_queue_low", example_id=self.id, celery=celery)

    @api.model
    def task_with_reference(self, task_uuid, **kwargs):
        task = 'task_with_reference'
        example_id = kwargs.get('example_id')
        res = self.env['celery.example.line'].create({
            'name': task,
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [%s].' % (self._name, task)
        _logger.info(msg)
        return {'result': msg, 'res_model': 'celery.example.line', 'res_ids': [res.id]}

    @api.model
    def task_immediate(self, task_uuid, **kwargs):
        task = 'task_immediate'
        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'name': task,
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [%s].' % (self._name, task)
        _logger.info(msg)
        return msg

    @api.model
    def task_with_error(self, task_uuid, **kwargs):
        task = 'task_with_error'
        _logger.critical('RETRY of %s' % task)

        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [%s].' % (self._name, task)
        _logger.info(msg)
        return msg

    @api.model
    def task_queue_default(self, task_uuid, **kwargs):
        task = 'task_queue_default'
        example_id = kwargs.get('example_id')
        self.env['celery.example.line'].create({
            'name': task,
            'example_id': example_id
        })
        msg = 'CELERY called task: model [%s] and method [%s].' % (self._name, task)
        _logger.info(msg)
        return msg

    @api.model
    def task_queue_high(self, task_uuid, **kwargs):
        time.sleep(2)
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

    def _cron_schedule_example(self):
        self.env['celery.task'].call_task(self._name, 'schedule_cron_example')

    @api.model
    def schedule_cron_example(self, task_uuid, **kwargs):
        self.env['celery.task'].call_task(
            self._name, 'run_cron_example')

        msg = 'Schedule Cron Example'
        _logger.critical(msg)
        return {'result': msg}

    @api.model
    def run_cron_example(self, task_uuid, **kwargs):
        msg = 'Run Cron Example'
        _logger.critical(msg)
        return {'result': msg}

    def refresh_view(self):
        return True


class CeleryExampleLine(models.Model):
    _name = 'celery.example.line'
    _description = 'Celery Example Line'

    name = fields.Char(required=True)
    example_id = fields.Many2one('celery.example', string='Example', required=True, ondelete='cascade')
