# 导入标准库模块
import os
import subprocess
import tomllib as toml

# 导入第三方库模块
import duckdb

# 导入自定义模块
from .fifolock import pool_lock
from ..mlp_exceptions import DependencyError

APP_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)


# 检查VC++依赖函数
def check_vc_redist(version:float | str):
    
    try:

        # 通过查询注册表来检查
        result = subprocess.run(
            [
                # reg query指令用于查询Windows注册表
                "reg",
                "query", 
                
                # Visual C++ Restributable 的注册表路径
                f"HKLM\\SOFTWARE\\WOW6432Node\\Microsoft\\VisualStudio\\{version}"
            ],

            # 捕获命令的标准输出和标准错误输出
            capture_output=True, 

            # 表示将输出解码为字符串
            text=True, 

            # 表示命令执行失败不会抛出异常
            check=False
        )

        # 如果命令返回码为0，说明找到了注册表项
        if result.returncode == 0:
            return True
        else:
            return False

    except Exception as e:
        print(f"查询过程中出现错误: {e}")
        return False


# 初始化服务函数
# 该函数负责：
    # 检查系统依赖
    # 初始化数据池
async def initialize(pool_meta: str):

    # 检查Visual C++ Redistributable是否安装
    if not check_vc_redist("14.0"):
        raise DependencyError(
            "Visual C++ Redistributable"
        )        

    # 读取数据池文件路径
    with open(
        f"{APP_ROOT}/config.toml", "rb"
    ) as file:

        config = toml.load(file)
    
    pool_path = config['data_pool_path']

    # 如果数据池文件不存在，则创建之
    if not os.path.exists(pool_path):

        if not os.path.exists(
            os.path.dirname(pool_path)
        ):
            os.makedirs(
                os.path.dirname(pool_path)
            )
        
        with duckdb.connect(pool_path) as conn:

            pass # pragma: no cover
    
    # 初始化数据池文件元信息表
    async with pool_lock:
        
        with duckdb.connect(pool_path) as conn:

            conn.sql(
                f"CREATE TABLE IF NOT EXISTS {pool_meta} " +
                f"(cell_id VARCHAR PRIMARY KEY," + 
                f"cell_name VARCHAR," +
                f"variables VARCHAR" + ");"
            )
    
    return pool_path

