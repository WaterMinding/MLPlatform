import uvicorn
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from docu import Docu

# 创建服务器程序对象
app = FastAPI()

# 创建文档对象引用（空）
document = None
 
# 配置 Jinja2 模板目录
templates = Jinja2Templates(directory = "templates")
app.mount("/static", StaticFiles(directory="templates"), name="static")
 
# 主界面接口
@app.get(path = "/", tags = ['获取主界面'])
async def read_root(request:Request):

    context = {"request": request}

    return templates.TemplateResponse("main.html", context)

@app.post(path = '/docu', tags = ['新建文档'])
async def create_docu(name:str):

    global document
    document = Docu(name)

    return{
        "docu_id":document.get_docu_id()
    }

@app.get(path = '/docu/id', tags = ['获取文档名称'])
async def get_docu_name():

    global document
    if document != None:

        return{
            "docu_id":document.get_docu_id()
        }
    
    else:

        return{
            "error":"当前无文档"
        }



if __name__ == "__main__":

    uvicorn.run("main:app",port = 8168,reload = True)