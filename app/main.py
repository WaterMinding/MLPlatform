# 导入第三方库模块
from fastapi import FastAPI
from fastapi import Body

# 导入自定义模块
from .services import initialize
from .services import open_docu
from .protocols import DocuConfig

# 创建FastAPI实例
app = FastAPI()

# 运行时数据池
data_pool = None

# 数据池文件路径
pool_path = None


# 新建文档接口
# 查询参数：docu_id - 文档ID
    # 文档ID由前端给出，是临时ID，
    # 用于区分不同文档的请求
@app.put("/docu")
async def new_docu_api(docu_id: str):

    global data_pool
    global pool_path
    
    # 调用初始化服务
    try:
        
        data_pool = await initialize()
    
    except Exception as e:

        return {
            "docu_id": docu_id,
            "status": "error", 
            "message": str(e),
        }

    pool_path = data_pool.pool_path

    return {
        "docu_id": docu_id,
        "status": "success", 
    }


# 打开文档接口
    # 查询参数：docu_id - 文档ID
    # 文档ID由前端给出，是临时ID，
    # 用于区分不同文档的请求
@app.get("/docu")
async def open_docu_api(
    docu_id: str,
    docu_config = Body(...),
):

    global data_pool
    global pool_path
    
    # 调用打开文档函数
    try:

        data_pool,missing = await open_docu(
            docu_config = docu_config,
        )

    except Exception as e:

        return {
            "docu_id": docu_id,
            "status": "error",
            "message": str(e),
        }
    
    pool_path = data_pool.pool_path

    return {
        "docu_id": docu_id,
        "status": "success",
        "missing": missing,
    }