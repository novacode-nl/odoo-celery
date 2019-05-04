# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import uuid

from odoo.exceptions import ValidationError
from odoo.tools.misc import mute_logger
from odoo.tests.common import TransactionCase

from ..models.celery_task import CeleryTask, STATE_PENDING


class TestCeleryTask(TransactionCase):

    def setUp(self):
        super(TestCeleryTask, self).setUp()

    def test_check_pending_payload_id(self):
        Task = self.env['celery.task']

        payload_id = 'celery.task.dummy_method'

        vals = {
            'uuid': str(uuid.uuid4()),
            'user_id': self.env.user.id,
            'model': 'celery.task',
            'method': 'dummy_method',
            'payload_id': payload_id
        }

        task_1 = Task.create(vals)
        self.assertIsInstance(task_1, CeleryTask)
        
        vals['uuid'] = str(uuid.uuid4())
        task_2 = Task.create(vals)
        self.assertIsNone(task_2)
