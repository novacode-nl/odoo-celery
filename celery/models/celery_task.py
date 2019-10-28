# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import copy
from datetime import datetime, timedelta
import json
import logging
import os
import traceback
import uuid

from odoo import api, fields, models, registry, _
from odoo.addons.base_sparse_field.models.fields import Serialized
from odoo.exceptions import UserError
from odoo.tools import config

from ..odoo import call_task, TASK_DEFAULT_QUEUE
from ..fields import TaskSerialized

logger = logging.getLogger(__name__)

TASK_NOT_FOUND = 'NOT_FOUND'

STATE_PENDING = 'PENDING'
STATE_STARTED = 'STARTED'
STATE_RETRY = 'RETRY'
STATE_RETRYING = 'RETRYING'
STATE_FAILURE = 'FAILURE'
STATE_SUCCESS = 'SUCCESS'
STATE_JAMMED = 'JAMMED'
STATE_CANCEL = 'CANCEL'

STATES = [(STATE_PENDING, 'Pending'),
          (STATE_STARTED, 'Started'),
          (STATE_RETRY, 'Retry'),
          (STATE_RETRYING, 'Retrying'),
          (STATE_JAMMED, 'Jammed'),
          (STATE_FAILURE, 'Failure'),
          (STATE_SUCCESS, 'Success'),
          (STATE_CANCEL, 'Cancel')]

STATES_TO_JAMMED = [STATE_STARTED, STATE_RETRY, STATE_RETRYING]
STATES_TO_CANCEL = [STATE_PENDING, STATE_JAMMED]
STATES_TO_REQUEUE = [STATE_PENDING, STATE_RETRY, STATE_JAMMED, STATE_FAILURE]


CELERY_PARAMS = [
    'queue', 'retry', 'max_retries', 'interval_start', 'interval_step',
    'countdown', 'retry_countdown_setting', 'retry_countdown_add_seconds',
    'retry_countdown_multiply_retries_seconds']

RETRY_COUNTDOWN_ADD_SECONDS = 'ADD_SECS'
RETRY_COUNTDOWN_MULTIPLY_RETRIES = 'MUL_RETRIES'
RETRY_COUNTDOWN_MULTIPLY_RETRIES_SECCONDS = 'MUL_RETRIES_SECS'

RETRY_COUNTDOWN_SETTINGS = [
    (RETRY_COUNTDOWN_ADD_SECONDS, 'Add seconds to retry countdown'),
    (RETRY_COUNTDOWN_MULTIPLY_RETRIES, 'Multiply retry countdown * request retries'),
    (RETRY_COUNTDOWN_MULTIPLY_RETRIES_SECCONDS, 'Multiply retry countdown: retries * seconds'),
]


def _get_celery_user_config():
    user = (os.environ.get('ODOO_CELERY_USER') or config.misc.get("celery", {}).get('user') or config.get('celery_user'))
    password = (os.environ.get('ODOO_CELERY_PASSWORD') or config.misc.get("celery", {}).get('password') or config.get('celery_password'))
    sudo = (os.environ.get('ODOO_CELERY_SUDO') or config.misc.get("celery", {}).get('sudo') or config.get('celery_sudo'))
    return (user, password, sudo)


class CeleryTask(models.Model):
    _name = 'celery.task'
    _description = 'Celery Task'
    _inherit = ['mail.thread']
    _rec_name = 'uuid'
    _order = 'id DESC'

    uuid = fields.Char(string='UUID', readonly=True, index=True, required=True)
    queue = fields.Char(string='Queue', readonly=True, required=True, default=TASK_DEFAULT_QUEUE, index=True)
    user_id = fields.Many2one('res.users', string='User ID', required=True, readonly=True)
    company_id = fields.Many2one('res.company', string='Company', index=True, readonly=True)
    model = fields.Char(string='Model', readonly=True)
    method = fields.Char(string='Method', readonly=True)
    kwargs = TaskSerialized(readonly=True)
    ref = fields.Char(string='Reference', index=True, readonly=True)
    started_date = fields.Datetime(string='Start Time', readonly=True)
    # TODO REFACTOR compute and store, by @api.depends (replace all ORM writes)
    state_date = fields.Datetime(string='State Time', index=True, readonly=True)
    result = fields.Text(string='Result', readonly=True)
    exc_info = fields.Text(string='Exception Info', readonly=True)
    state = fields.Selection(
        selection='_selection_states',
        string="State",
        default=STATE_PENDING,
        required=True,
        readonly=True,
        index=True,
        track_visibility='onchange',
        help="""\
        - PENDING: The task is waiting for execution.
        - STARTED: The task has been started.
        - RETRY: The task is to be retried, possibly because of failure.
        - RETRYING: The task is executing a retry, possibly because of failure.
        - JAMMED: The task has been Started, but due to inactivity marked as Jammed.
        - FAILURE: The task raised an exception, or has exceeded the retry limit.
        - SUCCESS: The task executed successfully.
        - CANCEL: The task has been aborted and cancelled by user action.""")
    res_model = fields.Char(string='Related Model', readonly=True)
    res_ids = fields.Serialized(string='Related Ids', readonly=True)

    # Celery Retry Policy
    # http://docs.celeryproject.org/en/latest/userguide/calling.html#retry-policy
    retry = fields.Boolean(default=True)
    max_retries = fields.Integer() # Don't default here (Celery already does)
    interval_start = fields.Float(
        help='Defines the number of seconds (float or integer) to wait between Broker Connection retries. '\
        'Default is 0 (the first retry will be instantaneous).') # Don't default here (Celery already does)
    interval_step = fields.Float(
        help='On each consecutive retry this number will be added to the Broker Connection retry delay (float or integer). '\
        'Default is 0.2.') # Don't default here (Celery already does)
    countdown = fields.Integer(help='ETA by seconds into the future. Also used in the retry.')
    retry_countdown_setting = fields.Selection(
        selection='_selection_retry_countdown_settings', string='Retry Countdown Setting')
    retry_countdown_add_seconds = fields.Integer(string='Retry Countdown add seconds')
    retry_countdown_multiply_retries_seconds = fields.Integer(string='Retry Countdown multiply retries seconds')

    def _selection_states(self):
        return STATES

    def _selection_retry_countdown_settings(self):
        return RETRY_COUNTDOWN_SETTINGS

    @api.multi
    def write(self, vals):
        celery_params = {param: vals[param] for param in CELERY_PARAMS if param in vals}

        if bool(celery_params):
            kwargs = self.kwargs and json.loads(self.kwargs) or {}
            if not kwargs.get('celery'):
                kwargs['celery'] = {}
            kwargs['celery'].update(celery_params)
            vals['kwargs'] = kwargs
        return super(CeleryTask, self).write(vals)

    @api.multi
    def unlink(self):
        for task in self:
            if task.state in [STATE_STARTED, STATE_RETRY]:
                raise UserError(_('You cannot delete a running task.'))
        super(CeleryTask, self).unlink()

    @api.model
    def call_task(self, model, method, **kwargs):
        """ Call Task dispatch to the Celery interface. """

        user, password, sudo = _get_celery_user_config()
        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)
        if not user_id:
            msg = _('The user "%s" doesn\'t exist.') % user
            logger.error(msg)
            return False
        
        user_id = user_id[0]['id']
        task_uuid = str(uuid.uuid4())
        vals = {
            'uuid': task_uuid,
            'user_id': user_id,
            'model': model,
            'method': method,
            # The task (method/implementation) kwargs, needed in the rpc_run_task model/method.
            'kwargs': kwargs}

        # queue selection
        default_queue = kwargs.get('celery', False) and kwargs.get('celery').get('queue', '') or 'celery'
        task_queue = False
        task_setting_domain = [('model', '=', model), ('method', '=', method), ('active', '=', True)]
        task_setting = self.env['celery.task.setting'].search(task_setting_domain)
        if task_setting:
            if task_setting.task_queue_ids:
                if task_setting.use_first_empty_queue:
                    for q in task_setting.task_queue_ids.sorted(key=lambda l: l.sequence):
                        if q.queue_id.active:
                            if self.search_count([('queue', '=', q.queue_id.name), ('state', '=', STATE_PENDING)]) <= q.queue_max_pending_tasks:
                                # use the first queue that satisfies the criteria of N or less pending tasks
                                task_queue = q.queue_id.name
                                break
                if not task_queue:
                    # use the first active queue from the task settings
                    active_queues = task_setting.task_queue_ids.filtered(lambda q: q.queue_id.active)
                    if active_queues:
                        task_queue = active_queues[0].queue_id.name
        if not task_queue:
            # use the default queue specified in code if not defined in task settings
            task_queue = default_queue

        if not kwargs.get('celery'):
            kwargs['celery'] = {}
        kwargs['celery'].update({'queue': task_queue})

        # Supported apply_async parameters/options shall be stored in the Task model-record.
        celery_vals = copy.copy(kwargs.get('celery'))
        if celery_vals.get('retry_policy'):
            vals.update(celery_vals.get('retry_policy'))
            del celery_vals['retry_policy']
        vals.update(celery_vals)

        if kwargs.get('celery_task_vals'):
            celery_task_vals = copy.copy(kwargs.get('celery_task_vals'))
            vals.update(celery_task_vals)

        with registry(self._cr.dbname).cursor() as cr:
            env = api.Environment(cr, user_id, {})
            try:
                task = self.with_env(env).create(vals)
            except CeleryCallTaskException as e:
                logger.error(_('ERROR FROM call_task %s: %s') % (task_uuid, e))
                cr.rollback()
            except Exception as e:
                logger.error(_('UNKNOWN ERROR FROM call_task: %s') % (e))
                cr.rollback()
        self._celery_call_task(user_id, task_uuid, model, method, kwargs)

    @api.model
    def _celery_call_task(self, user_id, uuid, model, method, kwargs):
        user, password, sudo = _get_celery_user_config()
        url = self.env['ir.config_parameter'].sudo().get_param('celery.celery_base_url')
        _args = [url, self._cr.dbname, user_id, uuid, model, method]

        if not kwargs.get('_password'):
            kwargs['_password'] = password

        _kwargsrepr = copy.deepcopy(kwargs)
        _kwargsrepr['_password'] = '*****'
        _kwargsrepr = repr(_kwargsrepr)

        # TODO DEPRECATED compatibility to remove after v12
        celery_kwargs = kwargs.get('celery')
        if not celery_kwargs:
            kwargs['celery'] = {}
        elif celery_kwargs.get('retry') and not celery_kwargs.get('retry_policy'):
            # The retry_policy defines the retry of the Broker Connection by Celery.
            retry_policy = {}
            if celery_kwargs.get('max_retries'):
                retry_policy['max_retries'] = celery_kwargs.get('max_retries')
            if celery_kwargs.get('interval_start'):
                retry_policy['interval_start'] = celery_kwargs.get('interval_start')
            if celery_kwargs.get('interval_step'):
                retry_policy['interval_step'] = celery_kwargs.get('interval_step')
            kwargs['celery']['retry_policy'] = retry_policy
        
        call_task.apply_async(args=_args, kwargs=kwargs, kwargsrepr=_kwargsrepr, **kwargs['celery'])

    @api.model
    def rpc_run_task(self, task_uuid, model, method, *args, **kwargs):
        """Run/execute the task, which is a model method.

        The idea is that Celery calls this by Odoo its external API,
        whereas XML-RPC or a HTTP-controller.

        The model-method can either be called as user:
        - The "celery" (Odoo user) defined in the odoo.conf. This is the default, in case
        the "sudo" setting isn't configured in the odoo.conf.
        - "admin" (Odoo admin user), to circumvent model-access configuration for models
        which run/process task. Therefor add "sudo = True" in the odoo.conf (see: example.odoo.conf).
        """

        task_queue = kwargs.get('celery', False) and kwargs.get('celery').get('queue', '') or 'celery'
        task_ref = kwargs.get('celery_task_vals', False) and kwargs.get('celery_task_vals').get('ref', '') or ''
        logger.info('CELERY rpc_run_task uuid:{uuid} - model: {model} - method: {method} - ref: {ref} - queue: {queue}'.format(
            uuid=task_uuid,
            model=model,
            method=method,
            ref=task_ref,
            queue=task_queue))
        
        exist = self.search_count([('uuid', '=', task_uuid)])
        if exist == 0:
            msg = "Task doesn't exist (anymore). Task-UUID: %s" % task_uuid
            logger.error(msg)
            return (TASK_NOT_FOUND, msg)

        model_obj = self.env[model]
        task = self.search([('uuid', '=', task_uuid), ('state', 'in', [STATE_PENDING, STATE_RETRY])], limit=1)

        if not task:
            return ('OK', 'Task already processed')

        if task.retry and task.state == STATE_RETRY:
            vals = {'state': STATE_RETRYING, 'state_date': fields.Datetime.now()}
        else:
            vals = {'state': STATE_STARTED, 'started_date': fields.Datetime.now()}

        # Store state before execution.
        with registry(self._cr.dbname).cursor() as result_cr:
            env = api.Environment(result_cr, self._uid, {})
            task.with_env(env).write(vals)

        user, password, sudo = _get_celery_user_config()

        # TODO
        # Re-raise Exception if not called by XML-RPC, but directly from model/Odoo.
        # This supports unit-tests and scripting purposes.
        result = False
        response = False
        with registry(self._cr.dbname).cursor() as cr:
            # Transaction/cursror for the exception handler.
            env = api.Environment(cr, self._uid, {})
            try:
                if bool(sudo) and sudo:
                    res = getattr(model_obj.with_env(env).sudo(), method)(task_uuid, **kwargs)
                else:
                    res = getattr(model_obj.with_env(env), method)(task_uuid, **kwargs)

                if res != False and not bool(res):
                    msg = "No result/return value for Task UUID: %s. Ensure the task-method returns a value." % task_uuid
                    logger.error(msg)
                    raise CeleryTaskNoResultError(msg)

                if isinstance(res, dict):
                    result = res.get('result', True)
                    vals.update({'res_model': res.get('res_model'), 'res_ids': res.get('res_ids')})
                else:
                    result = res
                vals.update({'state': STATE_SUCCESS, 'state_date': fields.Datetime.now(), 'result': result, 'exc_info': False})
            except Exception as e:
                """ The Exception-handler does a rollback. So we need a new
                transaction/cursor to store data about RETRY and exception info/traceback. """

                exc_info = traceback.format_exc()
                if task.retry:
                    state = STATE_RETRY
                    logger.warning('Retry... exception (see task form) from rpc_run_task {uuid} (model: {model} - method: {method} - ref: {ref} - queue: {queue}): {exc}.'.format(
                        uuid=task_uuid,
                        model=model,
                        method=method,
                        ref=task_ref,
                        queue=task_queue,
                        exc=e))
                else:
                    state = STATE_FAILURE
                    logger.warning('Failure... exception (see task form) from rpc_run_task {uuid} (model: {model} - method: {method} - ref: {ref} - queue: {queue}): {exc}.'.format(
                        uuid=task_uuid,
                        model=model,
                        method=method,
                        ref=task_ref,
                        queue=task_queue,
                        exc=e))
                vals.update({'state': state, 'state_date': fields.Datetime.now(), 'exc_info': exc_info})
                logger.debug('Exception rpc_run_task: {exc_info}'.format(uuid=task_uuid, exc_info=exc_info))
                cr.rollback()
            finally:
                with registry(self._cr.dbname).cursor() as result_cr:
                    env = api.Environment(result_cr, self._uid, {})
                    task.with_env(env).write(vals)
                response = (vals.get('state'), result)
                return response

    @api.model
    def rpc_set_state(self, task_uuid, state):
        """Set state of task, which is a model method.

        The idea is that Celery calls this by Odoo its external API,
        whereas XML-RPC or a HTTP-controller.
        """
        # TODO DRY: Also in rpc_run_task.
        # Move into separate method.
        exist = self.search_count([('uuid', '=', task_uuid)])
        if exist == 0:
            msg = "Task doesn't exist (anymore). Task-UUID: %s" % task_uuid
            logger.error(msg)
            return (TASK_NOT_FOUND, msg)
        
        task = self.search([('uuid', '=', task_uuid), ('state', '!=', state)], limit=1)
        if task:
            task.state = state
            msg = 'Update task state to: {state}'.format(state=state)
            return ('OK', msg)
        else:
            msg = 'Task already in state {state}.'.format(state=state)
            return ('OK', msg)

    @api.multi
    def action_pending(self):
        for task in self:
            task.state = STATE_PENDING
            task.started_date = None
            task.state_date = None
            task.result = None
            task.exc_info = None

    def _states_to_requeue(self):
        return STATES_TO_REQUEUE

    @api.multi
    def action_requeue(self):
        user, password, sudo = _get_celery_user_config()
        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)

        if not user_id:
            raise UserError('No user found with login: {login}'.format(login=user))
        user_id = user_id[0]['id']

        states_to_requeue = self._states_to_requeue()

        for task in self:
            if task.state in states_to_requeue:
                task.action_pending()
                try:
                    _kwargs = json.loads(task.kwargs)
                    self._celery_call_task(task.user_id.id, task.uuid, task.model, task.method, _kwargs)
                except CeleryCallTaskException as e:
                    logger.error(_('ERROR IN requeue %s: %s') % (task.uuid, e))
        return True

    def _states_to_cancel(self):
        return STATES_TO_CANCEL

    @api.multi
    def action_cancel(self):
        user, password, sudo = _get_celery_user_config()
        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)

        if not user_id:
            raise UserError('No user found with login: {login}'.format(login=user))
        user_id = user_id[0]['id']

        states_to_cancel = self._states_to_cancel()

        for task in self:
            if task.state in states_to_cancel:
                task.write({
                    'state': STATE_CANCEL,
                    'state_date': fields.Datetime.now()
                })
        return True

    @api.multi
    def action_jammed(self):
        user, password, sudo = _get_celery_user_config()
        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)

        if not user_id:
            raise UserError('No user found with login: {login}'.format(login=user))
        user_id = user_id[0]['id']

        for task in self:
            if task.state in STATES_TO_JAMMED:
                task.write({
                    'state': STATE_JAMMED,
                    'state_date': fields.Datetime.now()
                })
        return True

    @api.model
    def cron_handle_jammed_tasks(self):
        JammedTaskReport = self.env['celery.jammed.task.report']
        domain = [('jammed', '=', True)]
        tasks = JammedTaskReport.search(domain)
        for t in tasks:
            if t.handle_jammed and t.handle_jammed_by_cron:
                t.task_id.action_jammed()

    @api.model
    def autovacuum(self, **kwargs):
        # specify batch_size for high loaded systems
        batch_size = kwargs.get('batch_size', 100)
        days = kwargs.get('days', 90)
        hours = kwargs.get('hours', 0)
        minutes = kwargs.get('minutes', 0)
        seconds = kwargs.get('seconds', 0)

        success = kwargs.get('success', True)
        failure = kwargs.get('failure', True)
        cancel = kwargs.get('cancel', True)

        from_date = datetime.now() - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        states = [STATE_SUCCESS, STATE_FAILURE, STATE_CANCEL]
        if not success:
            states.remove(STATE_SUCCESS)
        if not failure:
            states.remove(STATE_FAILURE)
        if not cancel:
            states.remove(STATE_CANCEL)

        # state_date: because tasks could be created a while ago, but
        # finished much later.
        domain = [
            ('state_date', '!=', False), # extra safety check.
            ('state_date', '<=', from_date),
            ('state', 'in', states)
        ]

        # Remove tasks in a loop with batch_size step
        while True:
            tasks = self.search(domain, limit=batch_size)
            task_count = len(tasks)
            if not tasks:
                break
            else:
                tasks.unlink()
                logger.info('Celery autovacuum %s tasks', task_count)
            # Commit current step not to rollback the entire transation
            self.env.cr.commit()
        return True

    @api.multi
    def action_open_related_record(self):
        """ Open a view with the record(s) of the task.  If it's one record,
        it opens a form-view.  If it concerns mutltiple records, it opens
        a tree view.
        """

        self.ensure_one()
        model_name = self.res_model
        records = self.env[model_name].browse(self.res_ids).exists()
        if not records:
            return None
        action = {
            'name': _('Related Record'),
            'type': 'ir.actions.act_window',
            'view_type': 'form',
            'view_mode': 'form',
            'res_model': records._name,
        }
        if len(records) == 1:
            action['res_id'] = records.id
        else:
            action.update({
                'name': _('Related Records'),
                'view_mode': 'tree,form',
                'domain': [('id', 'in', records.ids)],
            })
        return action

    @api.multi
    def refresh_view(self):
        return True


class CeleryCallTaskException(Exception):
    """ CeleryCallTaskException """


class CeleryTaskNoResultError(Exception):
    """ CeleryCallTaskException """
