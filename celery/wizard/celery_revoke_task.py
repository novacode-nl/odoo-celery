# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models
from odoo.tools import config


class RevokeTask(models.TransientModel):
    _name = 'celery.revoke.task'
    _description = 'Celery Revoke Task Wizard'

    @api.model
    def _default_task_ids(self):
        res = False
        context = self.env.context
        Task = self.env['celery.task']
        states_to_revoke = Task._states_to_revoke()
        if (context.get('active_model') == 'celery.task' and
                context.get('active_ids')):
            task_ids = context['active_ids']
            res = Task.search([
                ('id', 'in', context['active_ids']),
                ('pid', '!=', 0),
                ('state', 'in', states_to_revoke)]).ids
        return res

    @api.model
    def _odoo_workers(self):
        return config.get('workers', 0)

    task_ids = fields.Many2many('celery.task', string='Tasks', default=_default_task_ids)
    odoo_workers = fields.Integer(default=_odoo_workers)

    @api.multi
    def action_revoke(self):
        self.task_ids.action_revoke()
        return {'type': 'ir.actions.act_window_close'}
