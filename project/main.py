from celery.result import AsyncResult
from celery import chain
from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import os

import worker as wk
import utils

DEFAULT_DUMP_FS = os.environ.get("DUMP_FILESTORE", False)
DEFAULT_DUMP_FORMAT = os.environ.get("DUMP_FORMAT", "sql")

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def home(request: Request):
    return JSONResponse({"request": "request"})


# @app.post("/tasks", status_code=201)
# def run_task(payload = Body(...)):
#     task_type = payload["type"]
#     task = wk.create_task.delay(int(task_type))
#     return JSONResponse({"task_id": task.id})


@app.get("/tasks/{task_id}")
def get_status(task_id):
    task_result = AsyncResult(task_id)

    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "all": [(t.name, t.status) for t in utils.iter_children(task_result)],
        # "task_result": task_result.result,
        # "tasks_status": [t.status for t in unpack_chain(task_result)],
    }
    # task_result.forget()


    return JSONResponse(result)


@app.post("/dump", status_code=201)
def run_task_dump(payload = Body(...)):
    data = {'db_name': payload["name"]}

    filestore = payload.get('filestore', DEFAULT_DUMP_FS)
    dump = payload.get('dump', DEFAULT_DUMP_FORMAT)

    tasks = chain(
        wk.create_env.s(data),
        wk.create_odoo_manifest.s(),
        wk.dump_db.s(),
        wk.add_to_zip.s(),
        # wk.add_filestore.s(data).set(link_error=wk.error_handler.s()),
        wk.add_filestore.s(),
        wk.clean_workdir.s(),
    ).on_error(wk.error_handler.s()).apply_async()

    result = {
        "task_id": tasks.id,
        "parent_id": [t.id for t in list(utils.unpack_parents(tasks))][-1],
        # "all": store(tasks)
    }
    return JSONResponse(result)



@app.get("/fast/{task_id}")
async def fast_download(task_id):

    try:
        filepath = utils._get_file_from_task(task_id)
    except ValueError as error:
        return JSONResponse({'status': error})

    return FileResponse(filepath)


@app.get("/download/{task_id}")
def download(task_id):

    try:
        filepath = utils._get_file_from_task(task_id)
    except ValueError as error:
        return JSONResponse({'status': error})

    return StreamingResponse(utils.iterfile(filepath), media_type="application/octet-stream")

@app.post("/restore", status_code=201)
def restore_backup(payload = Body(...)):
    data = {
        'db_name': payload["name"],
        'filename': payload["filename"]
    }

    # filestore = payload.get('filestore', DEFAULT_DUMP_FS)
    # dump = payload.get('dump', DEFAULT_DUMP_FORMAT)

    tasks = chain(
        wk.init_restore.s(data),
        # wk.unzip_dump.s(),
        # wk.create_database.s(),
        # wk.restore_dump.s(),
        wk.unzip_filestore.s(),
    ).apply_async()

    result = {
        "task_id": tasks.id,
        # "parent_id": [t.id for t in list(utils.unpack_parents(tasks))][-1],
        # "all": store(tasks)
    }
    return JSONResponse(result)