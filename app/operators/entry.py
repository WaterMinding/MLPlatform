# 导入标准库模块
import os
import sys
import importlib
import tomllib as toml

# 导入第三方库模块
from typeguard import typechecked

# 导入自定义模块
from .protocols import Operator
from ..mlp_exceptions import \
OperatorNotFoundError


# 获取当前脚本路径
FILE_PATH = os.path.abspath(__file__)

# 确定配置文件路径
OP_CONFIG_PATH = os.path.dirname(
    FILE_PATH
) + '/op_config.toml'


# 定义算子层入口函数
# 参数1：parameters - 算子参数字典
# 参数2：variables - 算子变量字典
@typechecked
def op_entry(
    op_name: str,
    parameters: dict | None,
    variables: dict | None, 
):
    
    # 读取配置文件
    with open(
        OP_CONFIG_PATH,
        mode = 'rb'
    ) as file:

        # 加载配置文件
        op_config = toml.load(file)

    # 获取算子目录
    op_dir = op_config['op_dict']
        
    # 检查算子名是否存在于算子目录
    if op_name not in op_dir:

        # 抛出异常
        raise OperatorNotFoundError(
            op_name
        )
    
    # 获取算子类
    op_str = op_dir[op_name]
    modu, clss = op_str.split('.')
    modu = importlib.import_module(
        '.' + modu,
        package = 'app.operators'
    )
    clss = getattr(modu, clss)

    # 检查算子类是否符合算子类型协议
    if not issubclass(clss, Operator):

        # 抛出异常
        raise TypeError(
            '算子类必须符合算子类型协议\n' +
            'Operator class must be subclass of Operator\
             which is a Protocol class'
        )
    
    # 构造算子对象
    op = clss(parameters, variables,)

    # 调用算子对象的run方法
    result = op.run()

    # 返回结果
    return result