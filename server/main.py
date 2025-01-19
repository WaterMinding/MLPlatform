from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from docu import Docu

# 创建服务器程序对象
app = FastAPI()

# 创建文档对象引用（空）
document = None
 
# 配置 Jinja2 模板目录
templates = Jinja2Templates(directory="../templates")
 
# 主界面接口
@app.get("/")
async def read_root(request: Request):

    context = {"request": request}

    return templates.TemplateResponse("main.html", context)

# 
