from datetime import datetime
import gzip
import os
from os.path import basename
from tempfile import NamedTemporaryFile, TemporaryDirectory, mkdtemp
from zipfile import ZipFile, ZIP_DEFLATED
# import psycopg2

# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg
import json
import shutil
from sh import pg_dump, psql
from celery.utils.log import get_task_logger

_logger = get_task_logger(__name__)

DEFAULT_DUMP_FILENAME = "dump.sql"
DEFAULT_DUMP_CMD = ["--no-owner"]
DEFAULT_MANIFEST_FILENAME = 'manifest.json'

POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = "db"
POSTGRES_PORT = "5432"

SQL_CREATE_DATABASE = 'CREATE DATABASE "{}";'
SQL_SELECT_MODULES = "SELECT name, latest_version FROM ir_module_module WHERE state = 'installed'"

OUTPUT_DIR = "/usr/src/output"
INPUT_DIR = "/usr/src/input"

def generate_filename(dbname):
    return "{}_{}".format(dbname, datetime.now().strftime("%Y%m%d_%H%M"))


def clean_workdir(path, files=[]):
    if not os.path.isdir(path):
        return True
    try:
        if not files:
            shutil.rmtree(path)
        else:
            for file in files:
                if os.path.exists(file):
                    os.remove(file)
    except:
        return False
    return True


def get_postgres_connection(dbname='postgres', **kwargs):
    # Connect to your postgres DB
    params = {
        'host': POSTGRES_HOST,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
        'dbname': dbname,
    }
    params.update(kwargs)
    # if dbname: params['dbname'] = dbname
    try:
        conn = psycopg.connect(**params)
    except psycopg.errors.OperationalError as err:
        message = err.diag.message_detail
        _logger.error(err)
        raise Exception(message)

    return conn

def create_database(db_name):
    # db.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with get_postgres_connection(autocommit=True) as conn:
        cr = conn.cursor()
        cr.execute(SQL_CREATE_DATABASE.format(db_name))

    return True

def guess_odoo_version(modules):
    try:
        return str(float(next(iter(modules.values())).split('.')[0]))
    except:
        return ""

def dump_db_manifest(cr):
    info = cr.connection.info
    pg_version = "%d.%d" % divmod(info.server_version / 100, 100)
    cr.execute(SQL_SELECT_MODULES)
    modules = dict(cr.fetchall())
    version = guess_odoo_version(modules)

    manifest = {
        'odoo_dump': '1',
        'db_name': info.get_parameters().get('dbname', False),
        'version': version,
        'version_info': version,
        'major_version': version,
        'pg_version': pg_version,
        'modules': modules,
    }
    return manifest



def _get_postgres_env():
    return {
        'PGHOST': POSTGRES_HOST,
        'PGPORT': POSTGRES_PORT,
        'PGUSER': POSTGRES_USER,
        'PGPASSWORD': POSTGRES_PASSWORD,
    }

def create_db_dump(db_name, path, filename=DEFAULT_DUMP_FILENAME, cmd=[]):

    args = DEFAULT_DUMP_CMD
    len(cmd) and args.append(*cmd)
    args.append(db_name)
    filepath = os.path.join(path, filename)

    with gzip.open(filepath, "wb") as f:
        pg_dump(*args, _out=f, _env=_get_postgres_env())

    stats = os.stat(filepath)

    return (True, {'path': filepath, 'size': stats.st_size})


def restore_db_dump(db_name, filepath, cmd=[]):

    if not os.path.isfile(filepath):
        raise FileNotFoundError(filepath)

    args = ["-U", POSTGRES_USER, "-d", db_name, "-f", filepath]

    psql(*args, _env=_get_postgres_env())

    stats = os.stat(filepath)

    return (True, {'path': filepath, 'size': stats.st_size})


def _check_path(path, raise_if_not_found=True):
    if not os.path.exists(path):
        if raise_if_not_found:
            raise FileNotFoundError(path)

def unzip_files(zipfile, files, **options):

    _check_path(zipfile)

    # tmp_dir = TemporaryDirectory(**options)
    tmp_dir = mkdtemp(**options)
    unzip_files = []

    with ZipFile(zipfile, 'r') as myzip:
        for f in files:
            try:
                myzip.extract(f, path=tmp_dir)
                new_file = os.path.join(tmp_dir, f)
                stats = os.stat(new_file)
                unzip_files.append({'path': new_file, 'size': stats.st_size})
            except KeyError:
                print("No file found: {}".format(f))
                continue

    return unzip_files


def unzip_filestore(zipfile, db_name, path):
    _check_path(zipfile)

    with TemporaryDirectory() as tmp_dir:
        with ZipFile(zipfile, 'r') as myzip:
            files = [f for f in myzip.namelist() if f.startswith('filestore/')]
            for f in files:
                myzip.extract(f, path=tmp_dir)
        shutil.move(os.path.join(tmp_dir, "filestore"), os.path.join(tmp_dir, db_name))
        shutil.move(os.path.join(tmp_dir, db_name), path)

    stats = os.stat(zipfile)
    return (True, {'path': path, 'size': stats.st_size})

def unzip_backup(zipfile, path):
    _check_path(zipfile)

    if not os.path.isdir(path):
        os.mkdir(path)

    with ZipFile(zipfile, 'r') as myzip:
        # Extract all the contents of zip file in different directory
        myzip.extractall(path)

    stats = os.stat(zipfile)
    return (True, {'path': path, 'size': stats.st_size})


def add_to_zip(files, zipfile, **kwargs):
    extension = '.zip'
    if not zipfile.endswith(extension):
        zipfile += extension

    options = {
        'compression': ZIP_DEFLATED,
        'allowZip64': True
    }
    options.update(kwargs)

    with ZipFile(zipfile, 'w', **options) as myzip:
        for filepath in files:
            filepath = os.path.normpath(filepath)
            filename = filepath[len(os.path.dirname(filepath))+1:]

            if os.path.isfile(filepath):
                myzip.write(filepath, filename)

    stats = os.stat(zipfile)

    return (True, {'path': zipfile, 'size':stats.st_size})


def create_odoo_manifest(path, db_name, filename=DEFAULT_MANIFEST_FILENAME):
    manifest = {}
    filepath = os.path.join(path, filename)
    with open(filepath, 'w') as fh:
        db = get_postgres_connection(db_name)
        with db.cursor() as cr:
            manifest = dump_db_manifest(cr)
            json.dump(manifest, fh, indent=4)

    return (filepath, manifest)


def add_folder_to_zip(path, zipfile, task=None):
    myzip = ZipFile(zipfile, 'a', compression=ZIP_DEFLATED, allowZip64=True)

    include_dir = True
    path = os.path.normpath(path)
    len_prefix = len(os.path.dirname(path)) if include_dir else len(path)
    if len_prefix:
        len_prefix += 1

    total = sum([len(files) for base, dirs, files in os.walk(path)])
    count = 0

    for dirpath, dirnames, filenames in os.walk(path):
        # filenames = sorted(filenames, key=fnct_sort)
        for fname in filenames:
            bname, ext = os.path.splitext(fname)
            ext = ext or bname
            if ext not in ['.pyc', '.pyo', '.swp', '.DS_Store']:
                path = os.path.normpath(os.path.join(dirpath, fname))
                count += 1
                if os.path.isfile(path):
                    myzip.write(path, path[len_prefix:])
            else:
                count -= 1
            progress = int((count * 100) / total)

            if task and progress in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
                task.update_state(state="PROGRESS", meta={'progress': progress})


    myzip.close()
    stats = os.stat(zipfile)

    return (True, {'path': zipfile, 'size':stats.st_size})
