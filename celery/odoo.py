# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

from celery import Celery
from celery.contrib import rdb
from celery.exceptions import TaskError, Retry, MaxRetriesExceededError
from celery.utils.log import get_task_logger
from xmlrpc import client as xmlrpc_client

logger = get_task_logger(__name__)

OK_CODE = 'OK'

# STATES (checks) should match with [celery.task] model!
STATE_RETRY = 'RETRY'
STATE_FAILURE = 'FAILURE'
TASK_NOT_FOUND = 'NOT_FOUND'


class TaskNotFoundInOdoo(TaskError):
    """The task doesn't exist (anymore) in Odoo (Celery Task model)."""

class RunTaskFailure(TaskError):
    """Error from rpc_run_task in Odoo."""


app = Celery('odoo.addons.celery')

@app.task(name='odoo.addons.celery.odoo.call_task', bind=True)
def call_task(self, url, db, user_id, password, task_uuid, _model_name, _method_name, **kwargs):
    odoo = xmlrpc_client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    args = [task_uuid, _model_name, _method_name]
    _celery_params = kwargs.get('celery')
    
    logger.info('{task_name} kwargs: {kwargs}'.format(task_name=self.name, kwargs=kwargs))
    logger.info('{task_name} celery_params: {celery_params}'.format(task_name=self.name, celery_params=_celery_params))

    try:
        response = odoo.execute_kw(db, user_id, password, 'celery.task', 'rpc_run_task', args, kwargs)

        if (isinstance(response, tuple) or isinstance(response, list)) and len(response) == 2:
            code = response[0]
            result = response[1]
        else:
            code = OK_CODE
            result = response

        if code == TASK_NOT_FOUND:
            msg = "%s, database: %s" % (result, db)
            raise TaskNotFoundInOdoo(msg)
        elif code in (STATE_RETRY, STATE_FAILURE):
            retry = _celery_params.get('retry')
            retry_policy = _celery_params.get('retry_policy')
            
            if retry and retry_policy:
                msg = 'Retry task... Failure from Odoo {db} (task: {uuid}, model: {model}, method: {method}).'.format(
                    db=db, uuid=task_uuid, model=_model_name, method=_method_name)
                logger.info(msg)

                params = {}
                if retry_policy['max_retries']:
                    params['max_retries'] = retry_policy['max_retries']
                if _celery_params.get('countdown'):
                    params['countdown'] = _celery_params.get('countdown')
                
                # Notify the worker to retry.
                logger.info('{task_name} retry params: {params}'.format(task_name=self.name, params=params))
                raise self.retry(**params)
            else:
                raise self.retry(max_retries=1, countdown=1)
        else:
            return (code, result)
    except Exception as e:
        """ A rather picky workaround to ignore/silence following exceptions.
        Only logs in case of other Exceptions.
        
        This also prevents concurrent retries causing troubles like
        concurrent DB updates (shall rollback) etc.

        - xmlrpc_client.Fault: Catches exception TypeError("cannot
        marshal None unless allow_none is enabled").  Setting
        allowd_none on the ServcerProxy won't work like expected and
        seems vague.
        - Retry: Celery exception notified to tell worker the task has
        been re-sent for retry.  We don't want to re-retry (double
        trouble here).

        See also odoo/service/wsgi_server.py for xmlrpc.client.Fault
        (codes), e.g: RPC_FAULT_CODE_CLIENT_ERROR = 1
        """
        if isinstance(e, MaxRetriesExceededError):
            # TODO
            # After implementation of "Hide sensitive data (password) by argspec/kwargspec, a re-raise should happen.
            # For now it shows sensitive data in the logs.
            msg = '[TODO] Failure (caught) MaxRetriesExceededError: db: {db}, task: {uuid}, model: {model}, method: {method}.'.format(
                db=db, uuid=task_uuid, model=_model_name, method=_method_name)
            logger.error(msg)
            # Task is probably in state RETRY. Now set it to FAILURE.
            args = [task_uuid, 'FAILURE']
            odoo.execute_kw(db, user_id, password, 'celery.task', 'rpc_set_state', args)
        elif not isinstance(e, Retry) and not isinstance(e, xmlrpc_client.Fault):
            # Maybe there's a way the send the xmlrpc.client.Fault into the Odoo exc_info field e.g.:
            # args = [xmlrpc_client.Fault.faultCode, xmlrpc_client.Fault.faultString]
            # odoo.execute_kw(db, user_id, password, 'celery.task', 'rpc_set_exception', args)
            #
            # Necessary to implement/call a retry() for other exceptions ?
            raise e
