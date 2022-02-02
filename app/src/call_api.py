from typing import List
import pandas as pd
import json
import os
import requests

from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import HTMLResponse

from io import BytesIO, StringIO

from csv_to_db import upload_to_db
from qc_validation import qc_validate, check_project_code

app = FastAPI()

@app.post("/uploadfile/")
async def create_upload_files(file: UploadFile = File(...), token: str = Form(...), key: str = Form(...)):
    response = await dump_file(key)
    if response['code'] != 200:
        return "wrong api key!"
    if file.filename.split('.')[-1] == 'csv':
        try:
            data = pd.read_csv(BytesIO(file.file.read()),dtype = 'str')
        except:
            data = pd.read_csv(BytesIO(file.file.read()),dtype = 'str', encoding='utf-8-sig')
    elif file.filename.split('.')[-1] == 'xlsx':
        try:
            data = pd.read_excel(BytesIO(file.file.read()),dtype = 'str')
        except:
            data = pd.read_excel(BytesIO(file.file.read()),dtype = 'str', encoding='utf-8-sig')
    else:
        return "error!, please upload correct file type"
    await upload_to_db(token,data)

    return "success"

@app.post("/resetcfg/")
async def reset_config():
    qc_file = open(os.path.join("config","qc_config.json"),'w+')
    qc_file.write('{}')
    print(qc_file)
    return 'clear config success'

@app.get('/reset/')
async def reset():
    content = """
<form action="/resetcfg/" enctype="multipart/form-data" method="post">
<h1>QC config reset</h1>
<p>press submit button to clear qc config</p>
<input type="submit">
</form>
</body>
"""

    return HTMLResponse(content=content)

@app.post("/bifrost/")
async def engine_input(request: Request):
    param = await request.json()
    if check_project_code(param['project_code']) != True:
        response = await dump_file(param['api_key'])
        if response['code'] != 200:
            return "wrong api key!"
        if check_project_code(param['project_code']) != True:
            return "project_code not found!"
    output = qc_validate(param['json_input'],param['project_code'])
    print(output)
    return output


@app.get("/")
async def main():
    content = """
<body>
<form action="/uploadfile/" enctype="multipart/form-data" method="post">
<h1>QC to MongoDB</h1>
<p>upload csv file and database name</p>
<p><input name="file" type="file"></p>
<p>Database name</p>
<p><input name="token" type="form"></p>
<p>API Key</p>
<p><input name="key" type="form"></p>
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)

async def dump_file(key):
    url = ""
    payload={}
    files={}
    headers = {'x-api-key': key}
    r = requests.request("GET", url, headers=headers, data=payload, files=files)
    response = r.json()
    #qc_config = await json.load(open(os.path.join("config","qc_config.json")))
    qc_config = await upload_json(open(os.path.join("config","qc_config.json")))
    print(qc_config)
    if response['code'] == 200:
        for i in response['data']:
            qc_config[i] = response['data'][i]
        with open(os.path.join("config","qc_config.json"), 'w+') as outfile:
            await dump_json(qc_config, outfile)
            return response

#### uvicorn call_api:app

async def upload_json(path):
    return json.load(path)

async def dump_json(qc_config, path):
    return json.dump(qc_config, path, ensure_ascii=False, indent=4)
