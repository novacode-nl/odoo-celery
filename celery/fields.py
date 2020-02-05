# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

import json

from odoo import fields, models


class KwargsSerialized(fields.Field):
    """ Serialized fields provide the storage for sparse fields. """
    type = 'task_serialized'
    column_type = ('text', 'text')

    def convert_to_column(self, value, record, values=None):
        return json.dumps(value)


class ListSerialized(fields.Field):
    type = 'list_serialized'
    column_type = ('text', 'text')

    def convert_to_column(self, value, record, values=None):
        return json.dumps(value)
