# -*- coding: utf-8 -*-
# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html)
{
    'name': 'Celery',
    'summary': 'Celery Distributed Task Queue',
    'category': 'Extra Tools',
    'version': '0.1',
    'description': """Execute Odoo task by Celery worker.""",
    'author': 'Nova Code',
    'website': 'https://www.novacode.nl',
    'license': "LGPL-3",
    'depends': ['mail'],
    'external_dependencies': {
        'python': ['celery'],
    },
    'data': [
        'views/celery_task_views.xml',
        'views/celery_menu.xml',
    ],
    'images': [
        'static/description/banner.png',
    ],
    'installable': True,
}
