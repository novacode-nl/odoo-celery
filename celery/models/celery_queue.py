# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models, _
from .celery_task import STATE_PENDING, STATE_RETRY, STATE_FAILURE, STATE_SUCCESS


class CeleryQueue(models.Model):
    _name = 'celery.queue'
    _description = 'Celery Queue'
    _inherit = ['mail.thread']
    _order = 'name'

    @api.multi
    def _get_task_settings(self):
        for record in self:
            task_setting_ids = self.env['celery.task.setting.queue'].search([('queue_id', '=', record.id)]) or []
            if task_setting_ids:
                task_setting_ids = [t.task_setting_id.id for t in task_setting_ids]
            record.task_setting_ids = task_setting_ids

    @api.multi
    def _compute_stats(self):
        self._cr.execute("SELECT queue, COUNT(*) FROM celery_task GROUP BY queue")
        queue_tasks = self._cr.fetchall()
        self._cr.execute("SELECT queue, COUNT(*) FROM celery_task WHERE state = %s GROUP BY queue", (STATE_PENDING, ))
        queue_tasks_pending = self._cr.fetchall()
        self._cr.execute("SELECT queue, COUNT(*) FROM celery_task WHERE create_date > (current_timestamp - interval '24 hour') GROUP BY queue")
        queue_tasks_24h = self._cr.fetchall()
        self._cr.execute("SELECT queue, COUNT(*) FROM celery_task WHERE create_date > (current_timestamp - interval '24 hour') AND state = %s GROUP BY queue", (STATE_SUCCESS, ))
        queue_tasks_24h_done = self._cr.fetchall()
        self._cr.execute("SELECT queue, COUNT(*) FROM celery_task WHERE create_date > (current_timestamp - interval '24 hour') AND (state = %s OR state = %s) GROUP BY queue", (STATE_FAILURE, STATE_RETRY))
        queue_tasks_24h_failed = self._cr.fetchall()

        total_tasks = sum([t[-1] for t in queue_tasks])
        queue_tasks = dict(queue_tasks)
        queue_tasks_24h = dict(queue_tasks_24h)
        queue_tasks_24h_done = dict(queue_tasks_24h_done)
        queue_tasks_24h_failed = dict(queue_tasks_24h_failed)
        queue_tasks_pending = dict(queue_tasks_pending)

        for record in self:
            record.queue_tasks = queue_tasks.get(record.name, 0)
            record.queue_tasks_pending = queue_tasks_pending.get(record.name, 0)
            record.queue_tasks_24h = queue_tasks_24h.get(record.name, 0)
            record.queue_tasks_24h_done = queue_tasks_24h_done.get(record.name, 0)
            record.queue_tasks_24h_failed = queue_tasks_24h_failed.get(record.name, 0)
            if queue_tasks_24h.get(record.name, 0) == 0: queue_tasks_24h[record.name] = 1  # avoid division by zero
            if total_tasks == 0: total_tasks = 1  # avoid division by zero
            record.queue_tasks_ratio = (float(queue_tasks.get(record.name, 0)) / float(total_tasks)) * 100.00
            record.queue_percentage = (float(queue_tasks_24h_done.get(record.name, 0)) / float(queue_tasks_24h.get(record.name, 0))) * 100.00

    name = fields.Char('Name', required=True, track_visibility='onchange')
    active = fields.Boolean(string='Active', default=True, track_visibility='onchange')
    task_setting_ids = fields.Many2many(
        string="Tasks",
        comodel_name="celery.task.setting",
        store=False,
        compute='_get_task_settings',
        help="Types of tasks that can be executed in this queue.",
    )
    
    # stat fields
    queue_tasks = fields.Integer(string="Total number of tasks in the queue", compute='_compute_stats', store=False, group_operator="avg")
    queue_tasks_pending = fields.Integer(string="Pending tasks in the queue", compute='_compute_stats', store=False, group_operator="avg")
    queue_tasks_24h = fields.Integer(string="Added in the last 24h", compute='_compute_stats', store=False, group_operator="avg")
    queue_tasks_24h_done = fields.Integer(string="Succeded in the last 24h", compute='_compute_stats', store=False, group_operator="avg")
    queue_tasks_24h_failed = fields.Integer(string="Failed in the last 24h", compute='_compute_stats', store=False, group_operator="avg")
    queue_percentage = fields.Float(string=" ", compute='_compute_stats', store=False, group_operator="avg")
    queue_tasks_ratio = fields.Float(string="Percentage of total tasks", compute='_compute_stats', store=False, group_operator="avg")
    color = fields.Integer('Color Index', default=0)

    _sql_constraints = [
        ('name_uniq', 'UNIQUE(name)', "Queue already exists!"),
    ]
