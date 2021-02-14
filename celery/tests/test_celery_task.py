# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import json
import uuid
import pytz

from odoo import fields
from odoo.exceptions import UserError, ValidationError
from odoo.tools.misc import mute_logger
from odoo.tests.common import TransactionCase

from ..models.celery_task import CeleryTask, STATE_PENDING, STATE_STARTED, STATE_RETRY


class TestCeleryTask(TransactionCase):

    def setUp(self):
        super(TestCeleryTask, self).setUp()

    def test_unlink_started_task(self):
        """ Unlink STARTED task """

        Task = self.env['celery.task']
        vals = {
            'uuid': str(uuid.uuid4()),
            'user_id': self.env.user.id,
            'model': 'celery.task',
            'method': 'dummy_method',
        }
        task = Task.create(vals)

        task.state = STATE_STARTED
        with self.assertRaisesRegex(UserError, 'You cannot delete a running task'), mute_logger('odoo.sql_db'):
            task.unlink()

    def test_unlink_retry_task(self):
        """ Unlink RETRY task """

        Task = self.env['celery.task']
        vals = {
            'uuid': str(uuid.uuid4()),
            'user_id': self.env.user.id,
            'model': 'celery.task',
            'method': 'dummy_method',
        }
        task = Task.create(vals)

        task.state = STATE_STARTED
        with self.assertRaisesRegex(UserError, 'You cannot delete a running task'), mute_logger('odoo.sql_db'):
            task.unlink()

    def test_write_task_update_celery_kwargs(self):
        """ Write task (Celery param fields) update Celery kwargs """

        Task = self.env['celery.task']

        vals = {
            'uuid': str(uuid.uuid4()),
            'user_id': self.env.user.id,
            'model': 'celery.task',
            'method': 'dummy_method',
            'kwargs': {'celery': {'retry': False,'countdown': 3}}
        }
        task = Task.create(vals)
        task.write({
            'retry': True,
            'max_retries': 5,
            'countdown': 10})

        kwargs = json.loads(task.kwargs)

        self.assertTrue(kwargs['celery']['retry'])
        self.assertEqual(kwargs['celery']['max_retries'], 5)
        self.assertEqual(kwargs['celery']['countdown'], 10)

    def test_scheduled_task(self):
        """ Creates a task setting scheduling the tasks to a time-window between 23:30 and 23:36 UTC """

        self.env['res.users'].sudo().browse(self.env.uid).tz = 'UTC'
        vals = {
            'model': 'celery.task',
            'method': 'dummy_method_schedule',
            'schedule': True,
            'schedule_hours_from': 23.5,
            'schedule_hours_to': 23.6,
        }
        task_setting = self.env['celery.task.setting'].create(vals)

        now = fields.datetime.now().replace(tzinfo=pytz.UTC)
        scheduled_date = self.env['celery.task'].check_schedule_needed(task_setting)

        if now.hour == 23 and now.minute > 30 and now.minute < 36:
            self.assertEqual(scheduled_date, False)
        else:
            self.assertEqual(scheduled_date.hour, 23)
            self.assertEqual(scheduled_date.minute, 30)
