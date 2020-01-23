# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from odoo import api, fields, models

LOCALHOST = 'http://127.0.0.1:8069'


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    celery_base_url = fields.Char('Celery Base URL')

    @api.model
    def get_values(self):
        res = super(ResConfigSettings, self).get_values()
        Param = self.env['ir.config_parameter'].sudo()
        res.update(
            celery_base_url=Param.get_param('celery.celery_base_url', default=LOCALHOST)
        )
        return res

    def set_values(self):
        super(ResConfigSettings, self).set_values()
        self.env['ir.config_parameter'].sudo().set_param(
            "celery.celery_base_url",
            self.celery_base_url)
