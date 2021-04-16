# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import contextlib

from odoo import api, models, tools
from odoo.addons.celery.models.celery_task import _get_celery_user_config


class Users(models.Model):
    _inherit = "res.users"

    @classmethod
    @tools.ormcache('uid', 'passwd')
    def check(cls, db, uid, passwd):
        """Verifies that the given (uid, password) is authorized for the database ``db`` and
           raise an exception if it is not."""

        # XXX (celery) copy from orignal with overrides
        if not passwd:
            # empty passwords disallowed for obvious security reasons
            raise AccessDenied()

        with contextlib.closing(cls.pool.cursor()) as cr:
            self = api.Environment(cr, uid, {})[cls._name]
            celery_user, celery_password, celery_sudo = _get_celery_user_config()
            if self.env.user.login != celery_user:
                # XXX (celery) if not celery user
                super(Users, cls).check(db, uid, passwd)
            with self._assert_can_auth():
                # XXX (celery) Altered
                # if not self.env.user.active:
                #     raise AccessDenied()-
                self._check_credentials(passwd, {'interactive': False})
