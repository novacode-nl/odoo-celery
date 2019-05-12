# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import logging

from odoo import tools
from odoo import api, fields, models, tools, _

from ..models.celery_task import STATE_JAMMED

_logger = logging.getLogger(__name__)


class CeleryJammedTaskReport(models.Model):
    _name = 'celery.jammed.task.report'
    _auto = False
    _rec_name = 'uuid'
    _order = 'task_id DESC'

    def _selection_states(self):
        return self.env['celery.task']._selection_states()

    task_id = fields.Many2one('celery.task', string='Celery Task', readonly=True)
    uuid = fields.Char(string='UUID', readonly=True)
    model = fields.Char(string='Model', readonly=True)
    method = fields.Char(string='Method', readonly=True)
    queue = fields.Char(string='Queue', readonly=True)
    state = fields.Selection(selection='_selection_states', readonly=True)
    started_date = fields.Datetime(string='Start Time', readonly=True)
    state_date = fields.Datetime(string='State Time', readonly=True)
    started_age_seconds = fields.Float(string='Started Age Seconds', readonly=True)
    state_age_seconds = fields.Float(string='State Age seconds', readonly=True)
    started_age_minutes = fields.Float(string='Started Age Minutes', readonly=True)
    state_age_minutes = fields.Float(string='State Age Minutes', readonly=True)
    started_age_hours = fields.Float(string='Started Age Hours', readonly=True)
    state_age_hours = fields.Float(string='State Age Hours', readonly=True)
    jammed = fields.Boolean(string='Seems Jammed', readonly=True)
    handle_by_cron = fields.Boolean(string='Handle by Cron', readonly=True)

    def _query(self):
        query_str = """
          WITH tasks AS (
            SELECT
              t.id AS id,
              t.uuid AS uuid,
              t.model AS model,
              t.method AS method,
              t.queue AS queue,
              t.state AS state,
              t.started_date AS started_date,
              t.state_date AS state_date,
              EXTRACT(EPOCH FROM timezone('UTC', now()) - t.started_date) AS started_age_seconds,
              EXTRACT(EPOCH FROM timezone('UTC', now()) - t.state_date) AS state_age_seconds,
              EXTRACT(EPOCH FROM timezone('UTC', now()) - t.started_date)/60 AS started_age_minutes,
              EXTRACT(EPOCH FROM timezone('UTC', now()) - t.state_date)/60 AS state_age_minutes,
              EXTRACT(EPOCH FROM timezone('UTC', now()) - t.started_date)/3600 AS started_age_hours,
              EXTRACT(EPOCH FROM timezone('UTC', now()) - t.state_date)/3600 AS state_age_hours,
              jts.jammed_seconds AS jammed_seconds,
              jts.handle_by_cron AS handle_by_cron
            FROM
              celery_task AS t
              LEFT JOIN celery_jammed_task_setting jts ON jts.model = t.model AND jts.method = t.method
            WHERE
              jts.active = True
          ),
          tasks_jammed AS (
            SELECT
              t.id AS id,
              t.id AS task_id,
              t.uuid AS uuid,
              t.model AS model,
              t.method AS method,
              t.queue AS queue,
              t.state AS state,
              t.started_date AS started_date,
              t.state_date AS state_date,
              t.started_age_seconds AS started_age_seconds,
              t.state_age_seconds AS state_age_seconds,
              t.started_age_minutes AS started_age_minutes,
              t.state_age_minutes AS state_age_minutes,
              t.started_age_hours AS started_age_hours,
              t.state_age_hours AS state_age_hours,
              (CASE
                 WHEN t.state = 'STARTED' AND t.jammed_seconds IS NOT NULL THEN t.started_age_seconds > t.jammed_seconds
                 WHEN t.state = 'RETRY' AND t.jammed_seconds IS NOT NULL THEN t.state_age_seconds > t.jammed_seconds
                 ELSE False
              END) AS jammed,
              t.handle_by_cron AS handle_by_cron
            FROM
              tasks AS t
          )
          SELECT
              t.id AS id,
              t.id AS task_id,
              t.uuid AS uuid,
              t.model AS model,
              t.method AS method,
              t.queue AS queue,
              t.state AS state,
              t.started_date AS started_date,
              t.state_date AS state_date,
              t.started_age_seconds AS started_age_seconds,
              t.state_age_seconds AS state_age_seconds,
              t.started_age_minutes AS started_age_minutes,
              t.state_age_minutes AS state_age_minutes,
              t.started_age_hours AS started_age_hours,
              t.state_age_hours AS state_age_hours,
              t.jammed AS jammed,
              t.handle_by_cron AS handle_by_cron
          FROM
            tasks_jammed AS t
          WHERE
            t.jammed = True
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
