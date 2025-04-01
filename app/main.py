# 导入标准库模块
import os
import tempfile
import tomllib as toml

# 导入第三方库模块
from fastapi import File
from fastapi import FastAPI
from fastapi import Request
from fastapi import UploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# 导入自定义模块
from .data import DataPool
from .services import run_op
from .services import initialize
from .services import open_docu
from .services import select_data
from .services import upload_data
from .services import get_pool_meta
from .services import delete_data
from .protocols import DocuConfig
from .protocols import OpConfigFront


# 创建FastAPI实例
app = FastAPI()

# 运行时数据池
DATA_POOL = None

# 数据池文件路径
POOL_PATH = None

# 数据池元信息表名称
POOL_META = "META_TABLE"

# app包根路径
APP_ROOT = os.path.dirname(
    os.path.abspath(__file__)
)

# 设置文件缓存路径
with open(
    f'{APP_ROOT}/config.toml',
    'rb'
) as file:
    
    config = toml.load(file)

    cache_path = config['data_cache_path']
    
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)

    tempfile.tempdir = cache_path


# 配置静态文件服务
app.mount(
    "/static",
    StaticFiles(
        directory = f"{APP_ROOT}/templates"
    ),
    name="static"
)

templates = Jinja2Templates(
    directory = f"{APP_ROOT}/templates"
)


# 根路由接口
@app.get(
    path = "/", 
    description = "全局初始化"
)
async def root(request: Request):

    global POOL_PATH

    # 调用初始化服务
    try:
        
        POOL_PATH = await initialize(
            POOL_META
        )
    
    except Exception as e:

        return {
            "status": "error", 
            "message": str(e),
        }
    
    return templates.TemplateResponse(
        "main.html",
        {"request": request}
    )
    

# 新建文档接口
    # 查询参数：docu_id - 文档临时ID
@app.post(
    path = "/docu/new", 
    description = "新建文档"
)
async def new_docu_api(docu_id: str):

    global DATA_POOL
    global POOL_PATH

    # 构造运行时数据池
    try:
        
        DATA_POOL = DataPool(
            POOL_PATH,
            POOL_META,
        )
    
    except Exception as e:
        
        return {
            "docu_id": docu_id,
            "status": "error",
            "message": str(e),
        }

    return {
        "docu_id": docu_id,
        "status": "success", 
    }


# 打开文档接口
    # 查询参数：docu_id - 文档临时ID
@app.post(
    path = "/docu/file", 
    description = "打开文档"
)
async def open_docu_api(
    docu_id: str,
    docu_config: DocuConfig,
):

    global DATA_POOL
    global POOL_PATH
    
    # 调用打开文档函数
    try:

        DATA_POOL,missing = await open_docu(
            docu_config = docu_config,
            pool_meta = POOL_META,
            pool_path = POOL_PATH,
        )

        pool_config = DATA_POOL.get_config()

    except Exception as e: 

        return {
            "docu_id": docu_id,
            "status": "error",
            "message": str(e),
        }

    return {
        "docu_id": docu_id,
        "status": "success",
        "pool_config": pool_config,
        "missing": missing,
    }


# 选择数据接口
    # 查询参数：docu_id - 文档临时ID
    # 查询参数：cell_id - 数据块ID
@app.patch(
    path = "/datapool/runtime", 
    description = "选择数据"
)
async def select_data_api(
    docu_id: str,
    cell_id: str,
):
    
    global DATA_POOL

    # 调用导入数据服务
    try:

        data_config = await select_data(
            data_pool = DATA_POOL,
            cell_id = cell_id,
        )

    except Exception as e:

        return {
            "docu_id": docu_id,
            "cell_id": cell_id,
            "status": "error",
            "message": str(e),
        }
    
    return {
        "docu_id": docu_id,
        "cell_id": cell_id,
        "status": "success",
        "data_config": data_config,
    }


# 上传数据接口
    # 查询参数：docu_id - 文档临时ID
    # 请求：file - 上传的文件
@app.patch(
    path = "/datapool/file",
    description = "上传数据"
)
async def upload_file_api(
    docu_id: str,
    file: UploadFile = File(...),
):
    
    # 调用上传文件服务
    try:

        new_meta = await upload_data(
            file = file,
            pool_path = POOL_PATH,
            pool_meta = POOL_META,
        )

    except Exception as e:

        return {
            "docu_id": docu_id,
            "status": "error",
            "message": str(e),
        }
    
    return {
        "docu_id": docu_id,
        "status": "success",
        "new_meta": new_meta,
    }
    

# 获取数据池文件元信息接口
    # 查询参数：docu_id - 文档临时ID
@app.get(
    path = "/datapool/file/meta",
    description = "获取数据池文件元信息"
)
async def get_pool_meta_api(
    docu_id: str,
):
    
    # 调用获取数据池文件元信息服务
    try:
        
        pool_meta = await get_pool_meta(
            pool_path = POOL_PATH,
            pool_meta = POOL_META,
        )
    
    except Exception as e:
        
        return {
            "docu_id": docu_id,
            "status": "error",
            "message": str(e),
        }
    
    return {
        "docu_id": docu_id,
        "status": "success",
        "pool_meta": pool_meta,
    }


# 删除数据池文件数据接口
    # 查询参数：docu_id - 文档临时ID
    # 查询参数：cell_id - 待删除数据库ID
@app.delete(
    path = "/datapool/file/cell",
    description = "删除数据池文件数据"
)
async def delete_pool_file_api(
    docu_id: str,
    cell_id: str,
):
    
    # 调用删除数据池文件服务
    try:
        
        cell_meta = await delete_data(
            pool_path = POOL_PATH,
            pool_meta = POOL_META,
            cell_id = cell_id,
        )

    except Exception as e:

        return {
            "docu_id": docu_id,
            "status": "error",
            "message": str(e),
        }
    
    return {
        "docu_id": docu_id,
        "status": "success",
        "cell_meta": cell_meta,
    }


# 算子执行接口
    # 查询参数：docu_id - 文档临时ID
    # 请求体：op_config - 算子配置
@app.post(
    path = "/op",
    description = "算子执行"
)
async def run_op_api(
    docu_id: str,
    op_config: OpConfigFront,
):

    # 调用算子执行服务
    try:

        op_result = await run_op(
            op_config = op_config,
            data_pool = DATA_POOL,
        )

    except Exception as e:

        return {
            "docu_id": docu_id,
            "status": "error",
            "message": str(e),
        }
        
    return {
        "docu_id": docu_id,
        "status": "success",
        "op_result": op_result,
    }