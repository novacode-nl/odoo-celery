# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from xmlrpc import client as xmlrpc_client

from celery import Celery
from celery.contrib import rdb
from celery.exceptions import TaskError


class TaskNotFoundInOdoo(TaskError):
    """The task doesn't exist (anymore) in Odoo (Celery Task model)."""

class RunTaskFailure(TaskError):
    """Error from run_task in Odoo."""


app = Celery('odoo.addons.celery')

@app.task(name='odoo.addons.celery.odoo.call_task')
def call_task(url, db, user_id, password, task_uuid, _model_name, _method_name, **kwargs):
    odoo = xmlrpc_client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    args = [task_uuid, _model_name, _method_name]
    response = odoo.execute_kw(db, user_id, password, 'celery.task', 'run_task', args, kwargs)
    code = response[0]
    result = response[1]
    
    if code == 'NOT_FOUND':
        msg = "%s, database: %s" % (result, db)
        raise TaskNotFoundInOdoo(msg)
    elif code == 'FAILURE':
        msg = "%s, database: %s" % (result, db)
        raise RunTaskFailure(msg)
    else:
        return response
