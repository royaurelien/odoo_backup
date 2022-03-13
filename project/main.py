from celery.result import AsyncResult
from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


from worker import create_task, dump_db
import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
# templates = Jinja2Templates(directory="templates")



@app.get("/")
def home(request: Request):
    return JSONResponse({"request": "request"})
    # return templates.TemplateResponse("home.html", context={"request": request})


@app.post("/tasks", status_code=201)
def run_task(payload = Body(...)):
    task_type = payload["type"]
    task = create_task.delay(int(task_type))
    return JSONResponse({"task_id": task.id})


@app.get("/tasks/{task_id}")
def get_status(task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JSONResponse(result)


@app.post("/dump", status_code=201)
def run_task_dump(payload = Body(...)):
    db_name = payload["name"]
    task = dump_db.delay(db_name)
    return JSONResponse({"task_id": task.id})


@app.get("/fast/{task_id}")
async def fast_download(task_id):

    task_result = AsyncResult(task_id)
    if not task_result.result:
        return JSONResponse({'status': 'No task found motherf****r !'})

    # result = eval(task_result.result)
    path = task_result.result[1].get('path', False)

    if not os.path.isfile(path):
        return JSONResponse({'status': 'No file found at {}'.format(path)})

    return FileResponse(path)


@app.get("/download/{task_id}")
def download(task_id):
    def iterfile(path):
        with open(path, mode="rb") as file_like:
            yield from file_like


    task_result = AsyncResult(task_id)
    if not task_result.result:
        return JSONResponse({'status': 'No task found motherf****r !'})

    # result = eval(task_result.result)
    path = task_result.result[1].get('path', False)

    if not os.path.isfile(path):
        return JSONResponse({'status': 'No file found at {}'.format(path)})

    return StreamingResponse(iterfile(path), media_type="application/octet-stream")