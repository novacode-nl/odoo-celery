# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models

from ..models.celery_task import STATES_TO_CANCEL


class CancelTask(models.TransientModel):
    _name = 'celery.cancel.task'
    _description = 'Celery Cancel Task Wizard'

    @api.model
    def _default_task_ids(self):
        states_to_cancel = self.env['celery.task']._states_to_cancel()
        res = False
        context = self.env.context
        if (context.get('active_model') == 'celery.task' and
                context.get('active_ids')):
            task_ids = context['active_ids']
            res = self.env['celery.task'].search([
                ('id', 'in', context['active_ids']),
                '|',
                ('state', 'in', states_to_cancel),
                ('stuck', '=', True)
            ]).ids
        return res

    task_ids = fields.Many2many('celery.task', string='Tasks', default=_default_task_ids)

    def action_cancel(self):
        self.task_ids.action_cancel()
        return {'type': 'ir.actions.act_window_close'}
