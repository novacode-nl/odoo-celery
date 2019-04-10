# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import logging

from odoo import tools
from odoo import api, fields, models, tools, _

from ..models.celery_task import STATES

_logger = logging.getLogger(__name__)


class CeleryTaskReport(models.Model):
    _name = 'celery.task.report'
    _auto = False
    _order = 'task_id DESC'

    task_id = fields.Many2one('celery.task', string='Celery Task', readonly=True)
    model = fields.Char(string='Model', readonly=True)
    method = fields.Char(string='Method', readonly=True)
    queue = fields.Char(string='Queue', readonly=True)
    state = fields.Selection(STATES, readonly=True)
    started_age_hours = fields.Float(string='Started Age Hours', readonly=True)
    state_age_hours = fields.Float(string='State Age Hours', readonly=True)
    jammed = fields.Boolean(string='Seems Jammed', readonly=True)

    def _query(self):
        query_str = """
          WITH jammed_time_window AS (
            SELECT
              value::float AS hours
            FROM
              ir_config_parameter
            WHERE
              key = 'celery.task.jammed.time.window'
          ),
          tasks AS (
            SELECT
              t.id AS id,
              t.model AS model,
              t.method AS method,
              t.queue AS queue,
              t.state AS state,
              EXTRACT(EPOCH FROM current_timestamp - t.started_date)/3600 AS started_age_hours,
              EXTRACT(EPOCH FROM current_timestamp - t.state_date)/3600 AS state_age_hours
            FROM
              celery_task AS t
          )
          SELECT
            --ROW_NUMBER() OVER() AS id,
            t.id AS id,
            t.id AS task_id,
            t.model AS model,
            t.method AS method,
            t.queue AS queue,
            t.state,
            t.started_age_hours,
            t.state_age_hours,
            (CASE t.state
               WHEN 'STARTED' THEN t.started_age_hours > (SELECT hours FROM jammed_time_window)
               WHEN 'RETRY' THEN t.state_age_hours > (SELECT hours FROM jammed_time_window)
               ELSE False
            END) AS jammed
          FROM
            tasks AS t
          ORDER BY
            t.id DESC

        """
        return query_str

    @api.model_cr
    def init(self):
        try:
            tools.drop_view_if_exists(self.env.cr, self._table)
            self.env.cr.execute("""CREATE or REPLACE VIEW %s as (%s)""" % (self._table, self._query()))
        except ValueError as e:
            msg = 'UPDATE the "celery" module. Required an initial data-import. Caught Exception: {exc}'.format(exc=e)
            _logger.critical(msg)
