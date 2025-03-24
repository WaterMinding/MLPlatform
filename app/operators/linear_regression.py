# 导入标准库模块
from copy import deepcopy
from typing import Protocol
from typing import TypedDict
from typing import runtime_checkable

# 导入第三方库模块
import pandas as pd
from pandas import DataFrame as DF
from typeguard import typechecked
from sklearn.linear_model import LinearRegression

# 导入自定义模块
from ..data import Variable


# 定义变量区字典协议
class VariableDict(TypedDict):

    X: Variable
    Y: Variable


# 线性回归类
class LR:

    # 构造方法
    # 参数1：params - 模型参数字典
        # 这个算子并不需要参数，
        # 但是为了params放在variables前面，
        # 这里没有使用默认值
    # 参数2：variables - 变量区字典
    @typechecked
    def __init__(
        self,
        params: None,
        variables: VariableDict,
    ):
        
        # 获取输入变量
        X = variables['X']
        Y = variables['Y']

        # 获取输入数据
        self.X_data = X.register
        self.Y_data = Y.register

        # 构造模型
        self.model = LinearRegression()

    # 执行方法    
    def run(self, *args, **kwargs):

        # 训练模型
        self.model.fit(
            self.X_data, 
            self.Y_data
        )

        # 获取模型参数
        coefficients = self.model.coef_ 
        intercept = self.model.intercept_

        # 构造模型公式
        formula = f'$$y = {coefficients[0][0]}x + {intercept[0]}$$'

        # 构造文本配置
        text_config = {
            'cell_num': 1,
            'text': formula
        }

        # 构造元素数据
        elem_data = pd.concat(
            [self.X_data, self.Y_data],
            axis=1
        )

        # 构造线配置
        line_config = {
            'elem_type': 'Line',
            'params': elem_data
        }

        # 构造散点配置
        scatter_config = {
            'elem_type': 'Scatter',
            'params': deepcopy(elem_data)
        }

        # 构造图表配置
        chart_config = {
            'cell_num': 2,
            'elem_list': [
                line_config, 
                scatter_config
            ]
        }

        # 构造结果
        result = {
            'text_list': [text_config],
            'chart_list': [chart_config],
            'data_list': None
        }

        return result

