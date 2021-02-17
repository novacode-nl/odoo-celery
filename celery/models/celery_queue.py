# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models, _
from .celery_task import STATE_PENDING, STATE_RETRY, STATE_FAILURE, STATE_SUCCESS


class CeleryQueue(models.Model):
    _name = 'celery.queue'
    _description = 'Celery Queue'
    _inherit = ['mail.thread']
    _order = 'name'

    def _get_task_settings(self):
        for record in self:
            task_setting_ids = self.env['celery.task.setting.queue'].search([('queue_id', '=', record.id)]) or []
            if task_setting_ids:
                task_setting_ids = [t.task_setting_id.id for t in task_setting_ids]
            record.task_setting_ids = task_setting_ids

    def _compute_stats(self):
        if 'compute_queue_stats' in self._context:
            self._cr.execute("""SELECT
                                    'total_tasks:all' AS queue_stat_field, COUNT(*) AS counted
                                    FROM celery_task
                                UNION
                                SELECT
                                    queue || ':all' AS queue_stat_field, COUNT(*) AS counted
                                    FROM celery_task
                                    GROUP BY queue
                                UNION
                                SELECT
                                    queue || ':pending' AS queue_stat_field, COUNT(*) AS counted
                                    FROM celery_task
                                    WHERE state = %s
                                    GROUP BY queue
                                UNION
                                SELECT
                                    queue || ':24h' AS queue_stat_field, COUNT(*) AS counted
                                    FROM celery_task
                                    WHERE create_date > (current_timestamp - interval '24 hour')
                                    GROUP BY queue
                                UNION
                                SELECT
                                    queue || ':24h_done' AS queue_stat_field, COUNT(*) AS counted
                                    FROM celery_task
                                    WHERE create_date > (current_timestamp - interval '24 hour')
                                        AND state = %s 
                                    GROUP BY queue
                                UNION
                                SELECT
                                    queue || ':24h_failed' AS queue_stat_field, COUNT(*) AS counted
                                    FROM celery_task
                                    WHERE create_date > (current_timestamp - interval '24 hour')
                                        AND (state = %s OR state = %s)
                                    GROUP BY queue
                                """, (STATE_PENDING, STATE_SUCCESS, STATE_FAILURE, STATE_RETRY))
            queue_tasks = self._cr.fetchall()

            queue_tasks = dict(queue_tasks)
            total_tasks = queue_tasks.get('total_tasks:all', 1)

            for record in self:
                record.queue_tasks = queue_tasks.get(record.name + ':all', 0)
                record.queue_tasks_pending = queue_tasks.get(record.name + ':pending', 0)
                record.queue_tasks_24h = queue_tasks.get(record.name + ':24h', 0)
                record.queue_tasks_24h_done = queue_tasks.get(record.name + ':24h_done', 0)
                record.queue_tasks_24h_failed = queue_tasks.get(record.name + ':24h_failed', 0)
                queue_tasks_24h = record.queue_tasks_24h
                if queue_tasks_24h == 0: queue_tasks_24h = 1  # avoid division by zero
                record.queue_tasks_ratio = (float(record.queue_tasks) / float(total_tasks)) * 100.00
                record.queue_percentage = (float(record.queue_tasks_24h_done) / float(queue_tasks_24h)) * 100.00

    name = fields.Char('Name', required=True, tracking=True)
    active = fields.Boolean(string='Active', default=True, tracking=True)
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
    queue_tasks_24h_done = fields.Integer(string="Succeeded in the last 24h", compute='_compute_stats', store=False, group_operator="avg")
    queue_tasks_24h_failed = fields.Integer(string="Failed in the last 24h", compute='_compute_stats', store=False, group_operator="avg")
    queue_percentage = fields.Float(string=" ", compute='_compute_stats', store=False, group_operator="avg")
    queue_tasks_ratio = fields.Float(string="Percentage of total tasks", compute='_compute_stats', store=False, group_operator="avg")
    color = fields.Integer('Color Index', default=0)

    _sql_constraints = [
        ('name_uniq', 'UNIQUE(name)', "Queue already exists!"),
    ]

    @api.model
    def cron_add_existing_queues(self):
        query = """
        INSERT INTO celery_queue (name, active) 
        SELECT DISTINCT queue, true FROM celery_task WHERE queue NOT IN (SELECT name FROM celery_queue)
        """
        self._cr.execute(query)
