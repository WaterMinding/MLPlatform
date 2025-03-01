import os
import uvicorn
import asyncio
import webbrowser
from docu import Docu
from pydantic import BaseModel
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
templates = Jinja2Templates(directory = "../templates")
app.mount("/static", StaticFiles(directory="../templates"), name="static")


# 获取主界面接口
@app.get(path = "/", tags = ['获取主界面'])
async def read_root_api(request:Request):

    context = {"request": request}

    return templates.TemplateResponse("main.html", context)


if __name__ == "__main__":

    webbrowser.open("http:\\172.25.80.1:8168")
    uvicorn.run("main:app",host = "172.25.80.1",port = 8168,reload = True)