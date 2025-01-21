import os
import uvicorn
import asyncio
import service
import webbrowser
from docu import Docu
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# 创建服务器程序对象
app = FastAPI()

# 创建文档对象引用（空）
document = None

# 创建共享资源锁
share_lock = asyncio.Lock()
 
# 配置 Jinja2 模板目录
templates = Jinja2Templates(directory = "templates")
app.mount("/static", StaticFiles(directory="templates"), name="static")


# 获取主界面接口
@app.get(path = "/", tags = ['获取主界面'])
async def read_root_api(request:Request):

    context = {"request": request}

    return templates.TemplateResponse("main.html", context)


# 新建文档接口
@app.post(path = '/docu', tags = ['新建文档'])
async def create_docu_api(docu_name:str = "untitled"):

    global document

    async with share_lock:
        document = await service.create_docu(docu_name)

    return{
        "docu_id":document.get_docu_id()
    }


if __name__ == "__main__":

    webbrowser.open("http:\\127.0.0.1:8168")
    uvicorn.run("main:app",port = 8168,reload = True)