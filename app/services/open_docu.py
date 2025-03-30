# 导入标准库模块
import os

# 导入第三方库模块
from duckdb import connect
from typeguard import typechecked

# 导入自定义模块
from ..data import DataPool
from .fifolock import pool_lock
from ..protocols import DocuConfig


# 打开文档函数
# 参数1：docu_config - 文档配置
async def open_docu(
    docu_config: DocuConfig,
    pool_path: str,
    pool_meta: str
):

    # 构造运行时数据池
    datapool = DataPool(
        pool_path,
        pool_meta
    )

    # 初始化数据缺失字典
    missing_data = {}

    data_list = docu_config.data_list

    # 获取数据池元信息表
    async with pool_lock:
        
        with connect(pool_path) as conn:

            meta_table = conn.table(
                pool_meta
            ).df()

            meta_list = meta_table['cell_id'].tolist()

    # 初始化运行时数据池
    for data_config in data_list:

        if data_config.cell_id in meta_list:

            datapool.add_cell(data_config)
        
        else:

            missing_data[
                data_config.cell_id
            ] = data_config.cell_name
                
    return datapool, missing_data