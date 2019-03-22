# -*- coding: utf-8 -*-
# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)
{
    'name': 'Celery',
    'summary': 'Celery (Distributed Task Queue)',
    'category': 'Extra Tools',
    'version': '0.4',
    'description': """Execute Odoo task by Celery worker.""",
    'author': 'Nova Code',
    'website': 'https://www.novacode.nl',
    'license': "LGPL-3",
    'depends': ['base'],
    'external_dependencies': {
        'python': ['celery'],
    },
    'data': [
        'security/celery_security.xml',
        'security/ir_model_access.xml',
        'views/celery_task_views.xml',
        'views/celery_menu.xml',
    ],
    'images': [
        'static/description/banner.png',
    ],
    'installable': True,
    'application' : True,
}
