import os
import uvicorn
import asyncio
import service
import webbrowser
from docu import Docu
from pydantic import BaseModel
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from exception import ParamTypeException
from exception import PathNotExistsException
from exception import RedundantOperationException
from exception import ObjectNotExistsException

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

    # 获取共享资源锁
    async with share_lock:
        
        # 创建文档
        document = await service.create_docu(docu_name)


    return{
        "docu_id":document.get_docu_id()
    }


# 增加块请求体模型
class Add_Cell_In(BaseModel):

    # 元数据字典
    meta_dict:dict = None

# 增加块接口
@app.post(path = '/cell', tags = ['块索引列表增加块'])
async def add_cell(in_body:Add_Cell_In = None,cell_type:str = 'Text'):

    global document

    # 处理请求体
    meta_dict = None
    if in_body != None:
        meta_dict = in_body.meta_dict
    
    # 获取共享资源锁
    async with share_lock:

        # 调用增加块函数
        try:
            cell = await service.add_cell(
                document = document,
                cell_type = cell_type,
                meta_dict = meta_dict,
            )
        except ObjectNotExistsException as one:
            return {"ObjectNotExistsException":"文档不存在"}
        except ParamTypeException as pte:
            return {"ParamTypeException":"块类型不正确"}

    # 返回新增块的状态
    return{
        "cell_id":cell.get_cell_id(),
        "meta_dict":cell.get_meta(),
    }



if __name__ == "__main__":

    webbrowser.open("http:\\127.0.0.1:8168")
    uvicorn.run("main:app",port = 8168,reload = True)