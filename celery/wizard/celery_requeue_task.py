# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models


class RequeueTask(models.TransientModel):
    _name = 'celery.requeue.task'
    _description = 'Celery Requeue Tasks Wizard'

    @api.model
    def _default_task_ids(self):
        res = False
        context = self.env.context
        Task = self.env['celery.task']
        states_to_requeue = Task._states_to_requeue()
        if (context.get('active_model') == 'celery.task' and
                context.get('active_ids')):
            task_ids = context['active_ids']
            res = Task.search([
                ('id', 'in', context['active_ids']),
                ('state', 'in', states_to_requeue)]).ids
        return res

    task_ids = fields.Many2many('celery.task', string='Tasks', default=_default_task_ids)

    @api.multi
    def action_requeue(self):
        self.task_ids.action_requeue()
        return {'type': 'ir.actions.act_window_close'}
