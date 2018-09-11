# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import copy
import logging
import os
import traceback
import uuid

from odoo import api, fields, models, registry, _
from odoo.addons.base_sparse_field.models.fields import Serialized
from odoo.exceptions import UserError
from odoo.tools import config

from ..odoo import call_task

logger = logging.getLogger(__name__)

TASK_NOT_FOUND = 'NOT_FOUND'

STATE_PENDING = 'PENDING'
STATE_STARTED = 'STARTED'
STATE_RETRY = 'RETRY'
STATE_FAILURE = 'FAILURE'
STATE_SUCCESS = 'SUCCESS'

STATES = [(STATE_PENDING, 'Pending'),
          (STATE_STARTED, 'Started'),
          (STATE_RETRY, 'Retry'),
          (STATE_FAILURE, 'Failure'),
          (STATE_SUCCESS, 'Success')]

def _get_celery_user_config():
    user = (os.environ.get('ODOO_CELERY_USER') or config.misc.get("celery", {}).get('user'))
    password = (os.environ.get('ODOO_CELERY_PASSWORD') or config.misc.get("celery", {}).get('password'))
    sudo = (os.environ.get('ODOO_CELERY_SUDO') or config.misc.get("celery", {}).get('sudo'))
    return (user, password, sudo)


class CeleryTask(models.Model):
    _name = 'celery.task'
    _description = 'Celery Task'
    # TODO Configure "Celery" group to access mail_thread ?
    #_inherit = ['mail.thread']
    _rec_name = 'uuid'
    _order = 'create_date DESC'

    uuid = fields.Char(string='UUID', readonly=True, index=True, required=True)
    user_id = fields.Many2one('res.users', string='User ID', required=True, readonly=True)
    company_id = fields.Many2one('res.company', string='Company', index=True, readonly=True)
    model = fields.Char(string='Model', readonly=True)
    method = fields.Char(string='Method', readonly=True)
    kwargs = fields.Serialized(readonly=True)
    started_date = fields.Datetime(string='Start Time', readonly=True)
    state_date = fields.Datetime(string='State Time', readonly=True)
    result = fields.Text(string='Result', readonly=True)
    exc_info = fields.Text(string='Exception Info', readonly=True)
    state = fields.Selection(
        STATES,
        string="State",
        default=STATE_PENDING,
        required=True,
        readonly=True,
        index=True,
        help="""\
        - PENDING: The task is waiting for execution.
        - STARTED: The task has been started.
        - RETRY: The task is to be retried, possibly because of failure.
        - FAILURE: The task raised an exception, or has exceeded the retry limit.
        - SUCCESS: The task executed successfully.""")
    res_model = fields.Char(string='Related Model', readonly=True)
    res_ids = fields.Serialized(string='Related Ids', readonly=True)

    # Celery Retry Policy
    # http://docs.celeryproject.org/en/latest/userguide/calling.html#retry-policy
    retry = fields.Boolean(default=True)
    max_retries = fields.Integer() # Don't default here (Celery already does)
    interval_start = fields.Float(
        help='Defines the number of seconds (float or integer) to wait between retries. '\
        'Default is 0 (the first retry will be instantaneous).') # Don't default here (Celery already does)
    interval_step = fields.Float(
        help='On each consecutive retry this number will be added to the retry delay (float or integer). '\
        'Default is 0.2.') # Don't default here (Celery already does)
    countdown = fields.Integer(help='ETA by seconds into the future. Also used in the retry.')

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

        if kwargs.get('celery'):
            # Supported apply_async parameters/options shall be stored in the Task model-record.
            celery_vals = kwargs.get('celery')
            vals.update(celery_vals)

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
        url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
        _args = [url, self._cr.dbname, user_id, password, uuid, model, method]

        celery = kwargs.get('celery')
        
        # Copy kwargs to update with more (Celery) info.
        if celery and celery.get('retry'):
            retry_policy = {}
            if celery.get('max_retries'):
                retry_policy['max_retries'] = celery.get('max_retries')
            if celery.get('interval_start'):
                retry_policy['interval_start'] = celery.get('interval_start')
            if celery.get('interval_step'):
                retry_policy['interval_step'] = celery.get('interval_step')

            # Provide kwargs with the Celery Task Parameters.
            kwargs['celery'] = {
                'retry': True,
                'retry_policy': retry_policy
            }

            # For now used in the retry countdown (not initial call).
            if celery.get('countdown'):
                kwargs['celery']['countdown'] = celery.get('countdown')
            # Call Celery Task.
            call_task.apply_async(args=_args, kwargs=kwargs, **kwargs['celery'])
        else:
            # Call Celery Task.
            call_task.apply_async(args=_args, kwargs=kwargs)

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

        logger.info('CELERY rpc_run_task uuid {uuid}'.format(uuid=task_uuid))
        
        exist = self.search_count([('uuid', '=', task_uuid)])
        if exist == 0:
            msg = "Task doesn't exist (anymore). Task-UUID: %s" % task_uuid
            logger.error(msg)
            return (TASK_NOT_FOUND, msg)

        model_obj = self.env[model]
        task = self.search([('uuid', '=', task_uuid), ('state', 'in', [STATE_PENDING, STATE_RETRY, STATE_FAILURE])], limit=1)

        if not task:
            return ('OK', 'Task already processed')

        # Start / Retry (refactor to absraction/neater code)
        celery_retry = kwargs.get('celery_retry')
        if celery_retry and task.retry and task.state == STATE_RETRY:
            return (STATE_RETRY, 'Task is already executing a retry.')
        elif celery_retry and task.celery_retry:
            task.state = STATE_RETRY
            vals = {'state': STATE_RETRY, 'state_date': fields.Datetime.now()}
        else:
            vals = {'state': STATE_STARTED, 'started_date': fields.Datetime.now()}

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
                vals.update({'state': STATE_RETRY, 'state_date': fields.Datetime.now(), 'exc_info': exc_info})
                logger.warning('Retry... exception (see task form) from rpc_run_task {uuid}: {exc}.'.format(uuid=task_uuid, exc=e))
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

    def set_state_pending(self):
        self.state = STATE_PENDING
        self.started_date = None
        self.state_date = None
        self.result = None
        self.exc_info = None

    @api.multi
    def requeue(self):
        user, password, sudo = _get_celery_user_config()
        user_id = self.env['res.users'].search_read([('login', '=', user)], fields=['id'], limit=1)

        if not user_id:
            raise UserError('No user found with login: {login}'.format(login=user))
        user_id = user_id[0]['id']

        for task in self:
            task.set_state_pending()
            try:
                self._celery_call_task(task.user_id.id, task.uuid, task.model, task.method, task.kwargs)
            except CeleryCallTaskException as e:
                logger.error(_('ERROR IN requeue %s: %s') % (task.uuid, e))
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


class RequeueTask(models.TransientModel):
    _name = 'celery.requeue.task'
    _description = 'Celery Requeue Tasks Wizard'

    @api.model
    def _default_task_ids(self):
        res = False
        context = self.env.context
        if (context.get('active_model') == 'celery.task' and
                context.get('active_ids')):
            task_ids = context['active_ids']
            res = self.env['celery.task'].search([
                ('id', 'in', context['active_ids']),
                ('state', 'in', ['PENDING', 'RETRY', 'FAILURE'])]).ids
        return res

    task_ids = fields.Many2many(
        'celery.task', string='Tasks', default=_default_task_ids,
        domain=[('state', 'in', ['PENDING', 'RETRY', 'FAILURE'])])

    @api.multi
    def requeue(self):
        self.task_ids.requeue()
        return {'type': 'ir.actions.act_window_close'}


class CeleryCallTaskException(Exception):
    """ CeleryCallTaskException """


class CeleryTaskNoResultError(Exception):
    """ CeleryCallTaskException """
