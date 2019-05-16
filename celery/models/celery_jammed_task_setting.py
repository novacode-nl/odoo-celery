# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models, _


class CeleryJammedTaskSetting(models.Model):
    _name = 'celery.jammed.task.setting'
    _description = 'Celery Jammed Task Setting'
    _inherit = ['mail.thread']
    _order = 'name'

    name = fields.Char('Name', compute='_compute_name', store=True)
    model = fields.Char(string='Model', required=True)
    method = fields.Char(string='Method', required=True)
    jammed_seconds = fields.Integer(
        string='Jammed After Seconds', required=True, track_visibility='onchange',
        help="A task seems Jammed when it's still in state STARTED or RETRY, after certain elapsed seconds.")
    handle_by_cron = fields.Boolean(
        string='Handle by Cron', default=False, track_visibility='onchange',
        help='Cron shall update Tasks which seems Jammed to state Jammed.')
    active = fields.Boolean(string='Active', default=True, track_visibility='onchange')

    _sql_constraints = [('model_method_unique', 'UNIQUE(model, method)', 'Combination of Model and Method already exists!')]

    @api.depends('model', 'method')
    def _compute_name(self):
        for r in self:
            r.name = '{model} - {method}'.format(model=r.model, method=r.method)
