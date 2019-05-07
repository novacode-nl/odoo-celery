# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import json
import uuid

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
