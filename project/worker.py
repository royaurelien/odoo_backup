from genericpath import isdir
import os
import time
from celery import Celery, chain
from celery.utils.log import get_task_logger
import uuid

import tools

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
celery.conf.result_extended = True
celery.conf.timezone = 'Europe/Paris'
celery.conf.enable_utc = True
celery.conf.task_annotations = {
    'create_env': {'rate_limit': '2/m'}
}

FILESTORE_PATH = '/usr/src/filestore'

_logger = get_task_logger(__name__)

@celery.task(name="error_handler")
def error_handler(request, exc, traceback):
    _logger.error("We've got serious problem here.")
    _logger.error(request)

    workdir = request.args[0].get('workdir', False)
    if workdir:
        res = tools.clean_workdir(workdir)
        _logger.warning("Clean workdir '{}': {}".format(workdir, res))

    # print('Task {0} raised exception: {1!r}\n{2!r}'.format(
        #   request.id, exc, traceback))


@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True


@celery.task(name="add_to_zip")
def add_to_zip(data):
    success, results = tools.add_to_zip(data.get('files'), data.get('zipfile'))
    data['zip'] = results
    data['download'] = results['path']

    return data


@celery.task(name="create_odoo_manifest")
def create_odoo_manifest(data):
    # time.sleep(30)
    workdir = data.get('workdir')
    db_name = data.get('db_name')

    filepath, manifest = tools.create_odoo_manifest(workdir, db_name)
    files = data.setdefault('files', [])
    files.append(filepath)

    return data


@celery.task(name="create_env")
def create_env(data):
    # time.sleep(10)
    workdir = os.path.join(tools.DATA_DIR, str(uuid.uuid4()))
    filename = tools.generate_filename(data.get('db_name'))
    zipfile = os.path.join(workdir, filename)


    if not os.path.isdir(workdir):
        os.mkdir(workdir)

    data.update({
        'workdir': workdir,
        'filename': filename,
        'zipfile': zipfile,
    })
    return data

@celery.task(name="dump_db")
def dump_db(data):
    success, results = tools.create_db_dump(data.get('db_name'), data.get('workdir'))

    data['dump'] = results
    files = data.setdefault('files', [])
    files.append(results['path'])

    return data

@celery.task(
    name="add_filestore",
    bind=True,
    max_retries=1,
    soft_time_limit=240
    )
def add_filestore(self, data):
    path = os.path.join(FILESTORE_PATH, data.get('db_name'))
    if not os.path.isdir(path):
        raise FileNotFoundError("Filestore '{}' not found.".format(path))
    success, results = tools.add_folder_to_zip(path, data['zip']['path'], task=self)

    return data

@celery.task(name="clean_workdir")
def clean_workdir(data):
    success = tools.clean_workdir(data.get('workdir'), data.get('files'))

    return data