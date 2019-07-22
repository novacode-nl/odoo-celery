# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import copy

from celery import Celery
from celery.contrib import rdb
from celery.exceptions import TaskError, Retry, MaxRetriesExceededError
from celery.utils.log import get_task_logger
from xmlrpc import client as xmlrpc_client

logger = get_task_logger(__name__)

TASK_DEFAULT_QUEUE = 'celery'

OK_CODE = 'OK'

# STATES (checks) should match with [celery.task] model!
STATE_RETRY = 'RETRY'
STATE_FAILURE = 'FAILURE'
TASK_NOT_FOUND = 'NOT_FOUND'

RETRY_COUNTDOWN_ADD_SECONDS = 'ADD_SECS'
RETRY_COUNTDOWN_MULTIPLY_RETRIES = 'MUL_RETRIES'
RETRY_COUNTDOWN_MULTIPLY_RETRIES_SECCONDS = 'MUL_RETRIES_SECS'


class TaskNotFoundInOdoo(TaskError):
    """The task doesn't exist (anymore) in Odoo (Celery Task model)."""

class RunTaskFailure(TaskError):
    """Error from rpc_run_task in Odoo."""


app = Celery('odoo.addons.celery')

@app.task(name='odoo.addons.celery.odoo.call_task', bind=True)
def call_task(self, url, db, user_id, task_uuid, model, method, **kwargs):
    odoo = xmlrpc_client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    args = [task_uuid, model, method]
    _kwargs = copy.deepcopy(kwargs)

    # Needed in the retry (call), to hide _password.
    _kwargsrepr = copy.deepcopy(kwargs)

    password = _kwargs.get('_password')
    del _kwargs['_password']
    celery_params = _kwargs.get('celery', {})

    logger.info('{model} {method} - celery.task uuid: {uuid}'.format(
        model=model, method=method, uuid=task_uuid))
    logger.info('{model} {method} - kwargs: {kwargs}'.format(
        model=model, method=method, kwargs=_kwargs))

    try:
        logger.info(
            'XML-RPC to Odoo server:\n\n'
            '- url: {url}\n'
            '- db: {db}\n'
            '- user_id: {user_id}\n'
            '- task_uuid: {task_uuid}\n'
            '- model: celery.task\n'
            '- method: rpc_run_task\n'
            '- args: {args}\n'
            '- kwargs {kwargs}\n'.format(
                url=url, db=db, user_id=user_id, task_uuid=task_uuid, model=model, method=method, args=args, kwargs=_kwargs))
        response = odoo.execute_kw(db, user_id, password, 'celery.task', 'rpc_run_task', args, _kwargs)

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
            retry = celery_params.get('retry')
            countdown = celery_params.get('countdown', 1)
            retry_countdown_setting = celery_params.get('retry_countdown_setting')
            retry_countdown_add_seconds = celery_params.get('retry_countdown_add_seconds', 0)
            retry_countdown_multiply_retries_seconds = celery_params.get('retry_countdown_multiply_retries_seconds', 0)

            # (Optionally) increase the countdown either by:
            # - add seconds
            # - countdown * retry requests
            # - retry requests * a given seconds
            if retry and retry_countdown_setting:
                if retry_countdown_setting == RETRY_COUNTDOWN_ADD_SECONDS:
                    countdown = countdown + retry_countdown_add_seconds
                elif retry_countdown_setting == RETRY_COUNTDOWN_MULTIPLY_RETRIES:
                    countdown = countdown * self.request.retries
                elif retry_countdown_setting == RETRY_COUNTDOWN_MULTIPLY_RETRIES_SECCONDS \
                     and retry_countdown_multiply_retries_seconds > 0:
                    countdown = self.request.retries * retry_countdown_multiply_retries_seconds
            celery_params['countdown'] = countdown
            
            if retry:
                msg = 'Retry task... Failure in Odoo {db} (task: {uuid}, model: {model}, method: {method}).'.format(
                    db=db, uuid=task_uuid, model=model, method=method)
                logger.info(msg)

                # Notify the worker to retry.
                logger.info('{task_name} retry params: {params}'.format(task_name=self.name, params=celery_params))
                _kwargsrepr['_password'] = '*****'
                _kwargsrepr = repr(_kwargsrepr)
                raise self.retry(kwargsrepr=_kwargsrepr, **celery_params)
            else:
                msg = 'Exit task... Failure in Odoo {db} (task: {uuid}, model: {model}, method: {method})\n'\
                      '  => Check task log/info in Odoo'.format(db=db, uuid=task_uuid, model=model, method=method)
                logger.info(msg)
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
                db=db, uuid=task_uuid, model=model, method=method)
            logger.error(msg)
            # Task is probably in state RETRY. Now set it to FAILURE.
            args = [task_uuid, 'FAILURE']
            odoo.execute_kw(db, user_id, password, 'celery.task', 'rpc_set_state', args)
        elif not isinstance(e, Retry):
            # Maybe there's a also a way the store a xmlrpc.client.Fault into the Odoo exc_info field e.g.:
            # args = [xmlrpc_client.Fault.faultCode, xmlrpc_client.Fault.faultString]
            # odoo.execute_kw(db, user_id, password, 'celery.task', 'rpc_set_exception', args)
            #
            # Necessary to implement/call a retry() for other exceptions ?
            msg = '{exception}\n'\
                  '  => SUGGESTIONS: Check former XML-RPC log messages.\n'.format(exception=e)
            logger.error(msg)
            raise e
