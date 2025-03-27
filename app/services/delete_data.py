# 导入标准库模块

# 导入第三方库模块
from duckdb import connect

# 导入自定义模块
from .initial import POOL_META
from .fifolock import pool_lock
from ..mlp_exceptions import DataNotFoundError

# 删除数据池中数据函数
async def delete_data(
    pool_path: str, 
    cell_id: str
):
    
    # 查询cell_id是否存在于数据池文件
    async with pool_lock:

        with connect(pool_path) as conn:

            cell_meta = conn.sql(
                f"SELECT * FROM {POOL_META} " + 
                f"WHERE cell_id = '{cell_id}'"
            ).df()

    if cell_meta.empty:

        raise DataNotFoundError(
            data_name = cell_id,
        )
    
    # 删除数据池文件中数据及元信息
    async with pool_lock:

        with connect(pool_path) as conn:

            conn.sql(
                f"DELETE FROM {POOL_META} " +
                f"WHERE cell_id = '{cell_id}'"
            )

            conn.sql(
                f"DROP TABLE {cell_id}"
            )

    return cell_meta.values[0].tolist()

