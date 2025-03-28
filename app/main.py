# 导入标准库模块
import os
import tempfile
import tomllib as toml

# 导入第三方库模块
from fastapi import File
from fastapi import FastAPI
from fastapi import UploadFile

# 导入自定义模块
from .data import DataPool
from .services import run_op
from .services import initialize
from .services import open_docu
from .services import import_data
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


# 根路由接口
@app.get("/")
async def root():

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
    

# 新建文档接口
    # 查询参数：docu_id - 文档临时ID
@app.post("/docu/new")
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
@app.post("/docu/file")
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


# 导入数据接口
    # 查询参数：docu_id - 文档临时ID
    # 查询参数：cell_id - 数据块ID
@app.patch("/datapool/runtime")
async def import_data_api(
    docu_id: str,
    cell_id: str,
):
    
    global DATA_POOL

    # 调用导入数据服务
    try:

        data_config = await import_data(
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
@app.patch("/datapool/file")
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
@app.get("/datapool/file/meta")
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
@app.delete("/datapool/file/cell")
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
@app.post("/op")
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