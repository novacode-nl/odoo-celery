# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html)

import xmlrpclib

from celery import Celery
from celery.contrib import rdb
from celery.exceptions import TaskError


class TaskNotFoundInOdooError(TaskError):
    """The task doesn't exist (anymore) in Odoo (Celery Task model)."""

class TaskRunOdooError(TaskError):
    """Error from run_task in Odoo."""


app = Celery('odoo.addons.celery')

@app.task(name='odoo.addons.celery.odoo.call_task')
def call_task(url, db, user_id, password, task_uuid, _model_name, _method_name, **kwargs):
    odoo = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
    args = [task_uuid, _model_name, _method_name]
    response = odoo.execute_kw(db, user_id, password, 'celery.task', 'run_task', args, kwargs)
    code = response[0]
    result = response[1]
    
    if code == 'NOT_FOUND':
        msg = "%s, database: %s" % (result, db)
        raise TaskNotFoundInOdooError(msg)
    elif code == 'ERROR':
        msg = "%s, database: %s" % (result, db)
        raise TaskRunOdooError(msg)
    else:
        return response
