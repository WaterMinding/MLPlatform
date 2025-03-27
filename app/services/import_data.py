# 导入标准库模块
import tomllib as toml

# 导入第三方库模块
from duckdb import connect
from pandas import DataFrame as DF

# 导入自定义模块
from ..data import DataPool
from .fifolock import pool_lock
from ..protocols import DataConfig
from ..mlp_exceptions import DataNotFoundError
from ..mlp_exceptions import DocuNotFoundError


# 导入数据函数
# 参数1：cell_id - 数据块ID
# 参数2：data_pool - 运行时数据池
async def import_data(
    cell_id: str,
    data_pool: DataPool | None
):

    # 如果运行时数据池为空，说明当前没有文档
    if data_pool is None:
        raise DocuNotFoundError()
    
    # 读取数据池文件元信息表
    async with pool_lock:
        
        with connect(data_pool.pool_path) as conn:

            meta = conn.sql(
                f"SELECT * FROM {data_pool.meta_name} " + 
                f"WHERE cell_id = '{cell_id}'"
            ).df()
    
    if meta.empty:

        raise DataNotFoundError(cell_id)

    # 获取数据块元信息
    cell_id = meta['cell_id'].iloc[0]
    cell_name = meta['cell_name'].iloc[0]
    variables = meta['variables'].iloc[0]

    # 解析变量长字符串
    var_str_list = variables.split(';')

    # 向运行时数据池中添加数据块
    data_config = DataConfig(
        cell_id = cell_id,
        cell_name = cell_name,
        var_str_list = var_str_list
    )

    data_pool.add_cell(data_config)

    # 返回数据块配置
    return data_config