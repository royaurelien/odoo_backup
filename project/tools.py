from datetime import datetime
import gzip
import os
from os.path import basename
from zipfile import ZipFile, ZIP_DEFLATED
import psycopg2
import json
import shutil
from sh import pg_dump

DEFAULT_DUMP_FILENAME = "dump.sql"
DEFAULT_DUMP_CMD = ["--no-owner"]
DEFAULT_MANIFEST_FILENAME = 'manifest.json'

POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = "db"
POSTGRES_PORT = "5432"

DATA_DIR = "/usr/src/data"

def generate_filename(dbname):
    return "{}_{}".format(dbname, datetime.now().strftime("%Y%m%d_%H%M"))

def create_zip(filename):
    pass

def clean_workdir(path):
    if not os.path.isdir(path):
        return True
    try:
        shutil.rmtree(path)
    except:
        return False
    return True


def get_odoo_database(dbname):
    # Connect to your postgres DB
    params = {
        'host': POSTGRES_HOST,
        'dbname': dbname,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
    }
    conn = psycopg2.connect(**params)
    # cr = conn.cursor()
    return conn

def dump_db_manifest(cr):
    pg_version = "%d.%d" % divmod(cr.connection.server_version / 100, 100)
    cr.execute("SELECT name, latest_version FROM ir_module_module WHERE state = 'installed'")
    modules = dict(cr.fetchall())
    manifest = {
        'odoo_dump': '1',
        'db_name': cr.connection.get_dsn_parameters().get('dbname', False),
        'version': 0,
        'version_info': 0,
        'major_version': 0,
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
        db = get_odoo_database(db_name)
        with db.cursor() as cr:
            manifest = dump_db_manifest(cr)
            json.dump(manifest, fh, indent=4)

    return (filepath, manifest)


def add_filestore_to_zip(zipfile, path):
    myzip = ZipFile(zipfile, 'a', compression=ZIP_DEFLATED, allowZip64=True)

    include_dir = True
    path = os.path.normpath(path)
    len_prefix = len(os.path.dirname(path)) if include_dir else len(path)
    if len_prefix:
        len_prefix += 1


    for dirpath, dirnames, filenames in os.walk(path):
        # filenames = sorted(filenames, key=fnct_sort)
        for fname in filenames:
            bname, ext = os.path.splitext(fname)
            ext = ext or bname
            if ext not in ['.pyc', '.pyo', '.swp', '.DS_Store']:
                path = os.path.normpath(os.path.join(dirpath, fname))
                if os.path.isfile(path):
                    myzip.write(path, path[len_prefix:])

    myzip.close()
