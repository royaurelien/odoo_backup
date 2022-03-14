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
    workdir = os.path.join(tools.OUTPUT_DIR, str(uuid.uuid4()))
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


@celery.task(name="init_restore")
def init_restore(data):
    filestore = os.path.join(FILESTORE_PATH, data.get('db_name'))
    if os.path.isdir(filestore):
        raise FileExistsError(filestore)
    # os.mkdir(filestore)

    zipfile = os.path.join(tools.INPUT_DIR, data.get('filename'))
    if not os.path.isfile(zipfile):
        raise FileNotFoundError(zipfile)

    data.update({
        'filestore': filestore,
        'zipfile': zipfile,
    })

    return data

@celery.task(name="unzip_dump")
def unzip_dump(data):
    unzip_files = tools.unzip_files(data.get('zipfile'), ['dump.sql'], prefix=data.get('db_name'))

    if not unzip_files:
        raise ValueError("No dump file found")

    data['dump'] = unzip_files[0]

    return data


@celery.task(name="restore_dump")
def restore_dump(data):
    # path = data['zip']['path']
    # search = ['dump.sql']

    # files = [os.path.join(path, f) for f in search if os.path.isfile(os.path.join(path, f))]
    # file = files[0] if files else False

    # if not file:
    #     raise FileNotFoundError("Files not found: {}".format(", ".join(search)))
    file = data['dump']['path']

    success, results = tools.restore_db_dump(data.get('db_name'), file)
    data['dump'] = results

    return data

@celery.task(name="unzip_backup")
def unzip_backup(data):
    success, results = tools.unzip_backup(data.get('zipfile'), data.get('filestore'))
    data['zip'] = results

    return data

@celery.task(name="unzip_filestore")
def unzip_filestore(data):
    success, results = tools.unzip_filestore(data.get('zipfile'), data.get('db_name'), FILESTORE_PATH)
    data['zip'] = results

    return data

@celery.task(name="create_database")
def create_database(data):
    success = tools.create_database(data.get('db_name'))

    return data