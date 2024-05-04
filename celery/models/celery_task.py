# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import copy
import json
import logging
import os
import traceback
import uuid
import pytz

from datetime import date, datetime, timedelta

from odoo import api, fields, models, registry, _
from odoo.exceptions import UserError
from odoo.modules import registry as model_registry
from odoo.tools import config

from ..odoo import call_task, TASK_DEFAULT_QUEUE
from ..fields import KwargsSerialized, ListSerialized

logger = logging.getLogger(__name__)

TASK_NOT_FOUND = 'NOT_FOUND'

STATE_PENDING = 'PENDING'
STATE_SCHEDULED = 'SCHEDULED'
STATE_STARTED = 'STARTED'
STATE_RETRY = 'RETRY'
STATE_RETRYING = 'RETRYING'
STATE_FAILURE = 'FAILURE'
STATE_SUCCESS = 'SUCCESS'
STATE_CANCEL = 'CANCEL'

STATES = [(STATE_PENDING, 'Pending'),
          (STATE_SCHEDULED, 'Scheduled'),
          (STATE_STARTED, 'Started'),
          (STATE_RETRY, 'Retry'),
          (STATE_RETRYING, 'Retrying'),
          (STATE_FAILURE, 'Failure'),
          (STATE_SUCCESS, 'Success'),
          (STATE_CANCEL, 'Cancel')]

STATES_TO_STUCK = [STATE_STARTED, STATE_RETRY, STATE_RETRYING]
STATES_TO_CANCEL = [STATE_PENDING, STATE_SCHEDULED]
STATES_TO_REQUEUE = [STATE_PENDING, STATE_SCHEDULED, STATE_RETRY, STATE_FAILURE]


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
    kwargs = KwargsSerialized(readonly=True)
    ref = fields.Char(string='Reference', index=True, readonly=True)
    started_date = fields.Datetime(string='Start Time', readonly=True)
    # TODO REFACTOR compute and store, by @api.depends (replace all ORM writes)
    state_date = fields.Datetime(string='State Time', index=True, readonly=True)
    scheduled_date = fields.Datetime(string="Scheduled Time", readonly=True)
    result = fields.Text(string='Result', readonly=True)
    exc_info = fields.Text(string='Exception Info', readonly=True, tracking=True)
    state = fields.Selection(
        selection='_selection_states',
        string="State",
        default=STATE_PENDING,
        required=True,
        readonly=True,
        index=True,
        tracking=True,
        help="""\
        - PENDING: The task is waiting for execution.
        - SCHEDULED: The task is pending and scheduled to be run within a specified timeframe.
        - STARTED: The task has been started.
        - RETRY: The task is to be retried, possibly because of failure.
        - RETRYING: The task is executing a retry, possibly because of failure.
        - FAILURE: The task raised an exception, or has exceeded the retry limit.
        - SUCCESS: The task executed successfully.
        - CANCEL: The task has been aborted and cancelled by user action.""")
    res_model = fields.Char(string='Related Model', readonly=True)
    res_ids = ListSerialized(string='Related Ids', readonly=True)
    stuck = fields.Boolean(string='Stuck')

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
    transaction_strategy = fields.Char(string='Transaction Strategy', readonly=True)

    def _selection_states(self):
        return STATES

    def _selection_retry_countdown_settings(self):
        return RETRY_COUNTDOWN_SETTINGS

    def write(self, vals):
        celery_params = {param: vals[param] for param in CELERY_PARAMS if param in vals}

        if bool(celery_params):
            kwargs = self.kwargs and json.loads(self.kwargs) or {}
            if not kwargs.get('celery'):
                kwargs['celery'] = {}
            kwargs['celery'].update(celery_params)
            vals['kwargs'] = kwargs
        return super(CeleryTask, self).write(vals)

    def unlink(self):
        for task in self:
            if task.state in [STATE_STARTED, STATE_RETRY]:
                raise UserError(_('You cannot delete a running task.'))
        super(CeleryTask, self).unlink()

    @api.model
    def check_schedule_needed(self, t_setting):
        # returns False if the task can be executed right now (current moment fits the scheduling criteria)
        # returns a datetime value (in UTC) for when it can be executed next (current moment does not fit the scheduling criteria)
        def get_next_day_of_week_diff(current_day_of_week, allowed_days):
            next_allowed_day_of_week = list(filter(lambda day: day > current_day_of_week, allowed_days))
            if next_allowed_day_of_week:
                # next day is this week
                return next_allowed_day_of_week[0] - current_day_of_week
            else:
                # next day is next week
                return 7 - current_day_of_week + list(filter(lambda day: day < current_day_of_week, allowed_days))[0]

        def get_next_hour_diff(current_hour, hour_from, hour_to, current_day_of_week, allowed_days):
            if current_hour <= hour_from:
                if not allowed_days or list(filter(lambda day: day == current_day_of_week, allowed_days)):
                    return hour_from - current_hour
                else:
                    return (hour_from - current_hour) + (get_next_day_of_week_diff(current_day_of_week, allowed_days) * 24)
            else:
                if not allowed_days:
                    return (24 - current_hour + hour_from)
                else:
                    return (24 - current_hour + hour_from) + (get_next_day_of_week_diff(current_day_of_week, allowed_days) * 24)

        scheduled_date = False
        user_tz = self.env['res.users'].sudo().browse(self.env.uid).tz
        tz = pytz.timezone(user_tz) if user_tz else pytz.utc
        current_day_of_week = datetime.now(tz=tz).weekday() + 1  # adding 1 to avoid comparisons to 0
        current_hour = (datetime.now(tz=tz).hour + (datetime.now(tz=tz).minute / 60.0) + (datetime.now(tz=tz).second / 3600.0)) or 0
        allowed_days = list(filter(None, [1 if t_setting.schedule_mondays else False,
                                          2 if t_setting.schedule_tuesdays else False,
                                          3 if t_setting.schedule_wednesdays else False,
                                          4 if t_setting.schedule_thursdays else False,
                                          5 if t_setting.schedule_fridays else False,
                                          6 if t_setting.schedule_saturdays else False,
                                          7 if t_setting.schedule_sundays else False]))
        weekday_scheduling_set = any([t_setting.schedule_mondays, t_setting.schedule_tuesdays, 
                                      t_setting.schedule_wednesdays, t_setting.schedule_thursdays,
                                      t_setting.schedule_fridays, t_setting.schedule_saturdays, t_setting.schedule_sundays])
        hour_scheduling_set = (t_setting.schedule_hours_from + t_setting.schedule_hours_to) > 0  # hourly schedule is set if hours set are different from 0
        out_of_allowed_day_range = (current_day_of_week not in allowed_days)
        out_of_allowed_hour_range = (current_hour < t_setting.schedule_hours_from or current_hour > t_setting.schedule_hours_to)
        if weekday_scheduling_set and out_of_allowed_day_range:
            hour_diff = get_next_hour_diff(0, t_setting.schedule_hours_from, t_setting.schedule_hours_to, current_day_of_week, allowed_days)
            t = datetime.now(tz=tz)
            scheduled_date = (t - timedelta(seconds=t.second + (t.minute * 60) + (t.hour * 3600)) + timedelta(hours=hour_diff)).astimezone(pytz.utc)
        elif weekday_scheduling_set and not out_of_allowed_day_range and hour_scheduling_set and out_of_allowed_hour_range:
            hour_diff = get_next_hour_diff(current_hour, t_setting.schedule_hours_from, t_setting.schedule_hours_to, current_day_of_week, allowed_days)
            scheduled_date = (datetime.now(tz=tz) + timedelta(hours=hour_diff)).astimezone(pytz.utc)
        elif not weekday_scheduling_set and hour_scheduling_set and out_of_allowed_hour_range:
            hour_diff = get_next_hour_diff(current_hour, t_setting.schedule_hours_from, t_setting.schedule_hours_to, current_day_of_week, allowed_days)
            scheduled_date = (datetime.now(tz=tz) + timedelta(hours=hour_diff)).astimezone(pytz.utc)
        return scheduled_date and scheduled_date.replace(tzinfo=None) or False

    @api.model
    def call_task(self, model, method, **kwargs):
        """ Call Task dispatch to the Celery interface. """

        user, password, sudo = _get_celery_user_config()
        res_users = self.env['res.users'].with_context(active_test=False)
        user_id = res_users.search_read([('login', '=', user)], fields=['id'], limit=1)
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

        scheduled_date = False
        # queue selection
        default_queue = kwargs.get('celery', False) and kwargs.get('celery').get('queue', '') or 'celery'
        task_queue = False
        task_setting_domain = [('model', '=', model), ('method', '=', method), ('active', '=', True)]
        task_setting = self.env['celery.task.setting'].sudo().search(task_setting_domain, limit=1)
        if task_setting:
            # check if the task needs to be scheduled in a specified timeframe
            if task_setting.schedule:
                scheduled_date = self.check_schedule_needed(task_setting)
                if scheduled_date:
                    vals.update({'state': STATE_SCHEDULED, 'scheduled_date': scheduled_date})

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

        # Set transaction strategy and apply call_task
        transaction_strategy = self._transaction_strategy(task_setting, kwargs)
        logger.debug('call_task - transaction strategy: %s' % transaction_strategy)
        dbname = self._cr.dbname
        vals['transaction_strategy'] = transaction_strategy

        def apply_call_task():
            # Closure uses several variables from enslosing scope.
            db_registry = model_registry.Registry.new(dbname)
            call_task = False
            with db_registry.cursor() as cr:
                env = api.Environment(cr, user_id, {})
                Task = env['celery.task']
                try:
                    task = Task.create(vals)
                    call_task = True
                except CeleryCallTaskException as e:
                    logger.error(_('ERROR FROM call_task %s: %s') % (task_uuid, e))
                    cr.rollback()
                    call_task = False
                except Exception as e:
                    logger.error(_('UNKNOWN ERROR FROM call_task: %s') % (e))
                    cr.rollback()
                    call_task = False

            if call_task:

                with db_registry.cursor() as cr:
                    env = api.Environment(cr, user_id, {})
                    Task = env['celery.task']
                    if not scheduled_date:  # if the task is not scheduled for a later time
                        Task._celery_call_task(user_id, task_uuid, model, method, kwargs)

        if transaction_strategy == 'immediate':
            apply_call_task()
        else:
            self._cr.commit()
            apply_call_task()

    def _transaction_strategies(self):
        transaction_strategies = self.env['celery.task.setting']._fields['transaction_strategy'].selection
        # return values except 'api'.
        return [r[0] for r in transaction_strategies if r[0] != 'api']

    def _transaction_strategy(self, task_setting, kwargs):
        """
        When the task shall apply (ORM create and send to Celery MQ).
        Possible options (return values):

        after_commit
        ------------
        The call_task shall apply after the main/caller transaction
        has been committed.  This avoids undesired side effects due to
        the current/main transaction isn't committed yet. Especially
        if you need to ensure data from the main transaction has been
        committed to use in the task.

        immediate
        ---------
        The call_task shall apply immediately from the main/caller
        transaction, even if it ain't committed yet.  Use wisely and
        ensure idempotency of the task. Because the main/caller
        transaction could fail and encounter a rollback, while the
        task shall still be send to the Celery MQ.
        """

        transaction_strategies = self._transaction_strategies()
        api_transaction_strategy = kwargs.get('transaction_strategy')

        if task_setting and task_setting.transaction_strategy == 'api' \
           and api_transaction_strategy in transaction_strategies:
            transaction_strategy = api_transaction_strategy
        elif not task_setting and api_transaction_strategy in transaction_strategies:
            transaction_strategy = api_transaction_strategy
        elif task_setting and task_setting.transaction_strategy != 'api':
            transaction_strategy = task_setting.transaction_strategy
        else:
            transaction_strategy = 'after_commit'

        # Extra save guard, in case the value is unknown.
        if transaction_strategy not in transaction_strategies:
            transaction_strategy = 'after_commit'

        return transaction_strategy

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
        task = self.search([('uuid', '=', task_uuid), ('state', 'in', [STATE_PENDING, STATE_RETRY, STATE_SCHEDULED])], limit=1)

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
                    if res.get('res_model'):
                        vals['res_model'] = res.get('res_model')
                    if res.get('res_ids'):
                        vals['res_ids'] = res.get('res_ids')
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

    def action_pending(self):
        for task in self:
            vals = {
                'state': STATE_PENDING,
                'started_date': None,
                'state_date': None,
                'result': None,
                'exc_info': None
            }
            if task.stuck:
                vals['stuck'] = False
            task.write(vals)

    def _states_to_requeue(self):
        return STATES_TO_REQUEUE

    def action_requeue(self):
        user, password, sudo = _get_celery_user_config()
        res_users = self.env['res.users'].with_context(active_test=False)
        user_id = res_users.search_read([('login', '=', user)], fields=['id'], limit=1)

        if not user_id:
            raise UserError('No user found with login: {login}'.format(login=user))
        user_id = user_id[0]['id']

        states_to_requeue = self._states_to_requeue()

        for task in self:
            if task.stuck or task.state in states_to_requeue:
                task.action_pending()
                try:
                    _kwargs = json.loads(task.kwargs)
                    self._celery_call_task(task.user_id.id, task.uuid, task.model, task.method, _kwargs)
                except CeleryCallTaskException as e:
                    logger.error(_('ERROR IN requeue %s: %s') % (task.uuid, e))
        return True

    def _states_to_cancel(self):
        return STATES_TO_CANCEL

    def action_cancel(self):
        user, password, sudo = _get_celery_user_config()
        res_users = self.env['res.users'].with_context(active_test=False)
        user_id = res_users.search_read([('login', '=', user)], fields=['id'], limit=1)

        if not user_id:
            raise UserError('No user found with login: {login}'.format(login=user))
        user_id = user_id[0]['id']

        states_to_cancel = self._states_to_cancel()

        for task in self:
            if task.stuck or task.state in states_to_cancel:
                vals = {'state': STATE_CANCEL, 'state_date': fields.Datetime.now()}
                if task.stuck:
                    vals['stuck'] = False
                task.write(vals)
        return True

    def action_stuck(self):
        user, password, sudo = _get_celery_user_config()
        res_users = self.env['res.users'].with_context(active_test=False)
        user_id = res_users.search_read([('login', '=', user)], fields=['id'], limit=1)

        if not user_id:
            raise UserError('No user found with login: {login}'.format(login=user))
        user_id = user_id[0]['id']

        for task in self:
            if task.state in STATES_TO_STUCK:
                task.write({
                    'stuck': True,
                    'state_date': fields.Datetime.now()
                })
        return True

    @api.model
    def cron_handle_stuck_tasks(self):
        StuckTaskReport = self.env['celery.stuck.task.report']
        domain = [('stuck', '=', True)]
        tasks = StuckTaskReport.search(domain)
        for t in tasks:
            if t.handle_stuck and t.handle_stuck_by_cron:
                t.task_id.action_stuck()

    @api.model
    def cron_handle_scheduled_tasks(self):
        domain = [('state', '=', STATE_SCHEDULED), ('scheduled_date', '<=', fields.Datetime.now())]
        tasks = self.search(domain)
        for t in tasks:
            t.action_requeue()

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

    def action_open_related_record(self):
        """ Open a view with the record(s) of the task.  If it's one record,
        it opens a form-view.  If it concerns mutltiple records, it opens
        a tree view.
        """

        self.ensure_one()
        model_name = self.res_model

        if not self.res_ids:
            raise UserError(_('Empty field res_ids'))
        res_ids = self.res_ids and json.loads(self.res_ids)

        records = self.env[model_name].browse(res_ids)
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

    def refresh_view(self):
        return True


class CeleryCallTaskException(Exception):
    """ CeleryCallTaskException """


class CeleryTaskNoResultError(Exception):
    """ CeleryCallTaskException """
