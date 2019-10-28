# Copyright Nova Code (http://www.novacode.nl)

def migrate(cr, version):
    query = """
    INSERT INTO celery_queue (name, active) 
    SELECT DISTINCT queue, true FROM celery_task WHERE queue NOT IN (SELECT name FROM celery_queue)
    """
    cr.execute(query)
