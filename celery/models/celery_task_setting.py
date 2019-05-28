# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models, _
from odoo.exceptions import ValidationError


class CeleryTaskSetting(models.Model):
    _name = 'celery.task.setting'
    _description = 'Celery Task Setting'
    _inherit = ['mail.thread']
    _order = 'name'

    name = fields.Char('Name', compute='_compute_name', store=True)
    model = fields.Char(string='Model', required=True, track_visibility='onchange')
    method = fields.Char(string='Method', required=True, track_visibility='onchange')
    handle_jammed = fields.Boolean(string="Handle Jammed", track_visibility='onchange')
    jammed_after_seconds = fields.Integer(
        string='Seems Jammed after seconds', track_visibility='onchange',
        help="A task seems Jammed when it's still in state STARTED or RETRY, after certain elapsed seconds.")
    handle_jammed_by_cron = fields.Boolean(
        string='Handle Jammed by Cron', default=False, track_visibility='onchange',
        help='Cron shall update Tasks which seems Jammed to state Jammed.')
    active = fields.Boolean(string='Active', default=True, track_visibility='onchange')

    @api.constrains('model', 'method')
    def _check_model_method_unique(self):
        count = self.search_count([('model', '=', self.model), ('method', '=', self.method)])
        if count > 1:
            raise ValidationError(_('Combination of Model and Method already exists!'))

    @api.depends('model', 'method')
    def _compute_name(self):
        for r in self:
            r.name = '{model} - {method}'.format(model=r.model, method=r.method)
