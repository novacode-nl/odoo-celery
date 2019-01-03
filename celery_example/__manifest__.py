# Copyright 2018 Nova Code (http://www.novacode.nl)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)
{
    'name': 'Celery Examples',
    'summary': 'Celery Example tasks ready to run from Odoo.',
    'category': 'Extra Tools',
    'version': '0.2',
    'description': """Put example tasks on the Celery Task Queue.""",
    'author': 'Nova Code',
    'website': 'https://www.novacode.nl',
    'license': "LGPL-3",
    'depends': ['celery'],
    'data': [
        'data/celery_example_data.xml',
        'security/ir_model_access.xml',
        'views/celery_example_views.xml'
    ],
    'images': [
        'static/description/banner.png',
    ],
    'installable': True,
    'application' : False,
}
