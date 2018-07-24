# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html)

import xmlrpclib

from celery import Celery
# from celery.contrib import rdb

app = Celery('odoo.addons.celery')

@app.task(name='odoo.addons.celery.celery_tasks.call_task')
def call_task(url, db, user_id, password, task_uuid, _model_name, _method_name, **kwargs):
    odoo = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
    args = [task_uuid, _model_name, _method_name]
    try:
        res = odoo.execute_kw(db, user_id, password, 'celery.task', 'run_task', args, kwargs)
        return res
    except Exception as e:
        # rdb.set_trace()
        # TODO raise Exception with more detail (user_id etc?)
        raise e
