# 导入标准库模块

# 导入第三方库模块
from duckdb import connect

# 导入自定义模块
from .fifolock import pool_lock


# 获取数据池文件元信息函数
async def get_pool_meta(
    pool_path: str,
    pool_meta: str
):

    # 获取元信息表
    async with pool_lock:    
        
        with connect(pool_path) as conn:

            meta = conn.sql(
                f"SELECT * FROM {pool_meta}"
            ).df()

    return meta.to_dict(orient='index')