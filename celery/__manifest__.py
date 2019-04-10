# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)
{
    'name': 'Celery',
    'summary': 'Celery (Distributed Task Queue)',
    'category': 'Extra Tools',
    'version': '0.6',
    'description': """Execute Odoo task by Celery worker.""",
    'author': 'Nova Code',
    'website': 'https://www.novacode.nl',
    'license': "LGPL-3",
    'depends': ['base'],
    'external_dependencies': {
        'python': ['celery'],
    },
    'data': [
        'data/celery_data.xml',
        'security/celery_security.xml',
        'security/ir_model_access.xml',
        'wizard/celery_task_handle_jammed_views.xml',
        'wizard/celery_task_requeue_views.xml',
        'wizard/celery_task_cancel_views.xml',
        'report/celery_task_report_views.xml',
        'views/celery_task_views.xml',
        'views/celery_menu.xml',
    ],
    'images': [
        'static/description/banner.png',
    ],
    'installable': True,
    'application' : True,
}
