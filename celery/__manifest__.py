# Copyright Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)
{
    'name': 'Celery',
    'summary': 'Celery (Distributed Task Queue)',
    'category': 'Extra Tools',
    'version': '17.0',
    'description': """Execute Odoo methods by Celery worker tasks.""",
    'author': 'Nova Code',
    'website': 'https://www.novacode.nl',
    'license': "LGPL-3",
    'depends': [
        'mail'
    ],
    'external_dependencies': {
        'python': ['celery'],
    },
    'data': [
        'data/ir_cron_data.xml',
        'data/ir_config_parameter_data.xml',
        'security/celery_security.xml',
        'security/ir_model_access.xml',
        'wizard/celery_requeue_task_views.xml',
        'wizard/celery_cancel_task_views.xml',
        'wizard/celery_handle_stuck_task_views.xml',
        'report/celery_stuck_task_report_views.xml',
        'views/celery_task_views.xml',
        'views/celery_task_setting_views.xml',
        'views/celery_queue_views.xml',
        'views/celery_menu.xml',
        'views/res_config_settings_views.xml',
    ],
    'images': [
        'static/description/banner.png',
    ],
    'installable': True,
    'application' : True,
}
