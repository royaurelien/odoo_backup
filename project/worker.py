import os
import time



from celery import Celery

from tools import generate_filename, create_db_dump, create_odoo_manifest

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")



@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True

@celery.task(name="dump_db")
def dump_db(db_name):
    # time.sleep(30)

    filename = generate_filename(db_name)
    res = create_odoo_manifest('./', db_name)
    res = create_db_dump(db_name, "{}_dump.sql".format(filename))

    return res