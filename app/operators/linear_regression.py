# 导入标准库模块
from copy import deepcopy
from typing import TypedDict

# 导入第三方库模块
import pandas as pd
from pandas import DataFrame as DF
from typeguard import typechecked
from sklearn.linear_model import LinearRegression

# 导入自定义模块
from ..data import Variable
from ..protocols import CellType
from ..protocols import ElemConfig
from ..protocols import ChartConfig
from ..protocols import TextConfig
from ..protocols import OpResult


# 定义变量区字典协议
class VariableDict(TypedDict):

    X: list[Variable]
    Y: list[Variable]


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
        X = variables['X'][0]
        Y = variables['Y'][0]

        # 获取输入数据
        self.X_data = X.register
        self.Y_data = Y.register

        # 检查数据与参数
        self.check()

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
        text_config = TextConfig(
            cell_type = CellType.TEXT,
            cell_num = 1,
            text = formula
        )

        # 构造散点元素数据
        scatter_data = pd.concat(
            [self.X_data, self.Y_data],
            axis=1
        )

        # 构造直线元素数据
        min_X_id = self.X_data.idxmin().to_list()[0]
        max_X_id = self.X_data.idxmax().to_list()[0]

        df_X = self.X_data.iloc[
            [min_X_id, max_X_id]
        ].reset_index(drop=True)

        df_Y = DF(self.model.predict(df_X))
        df_Y.columns = self.Y_data.columns
        df_Y[df_Y.columns[0]] = \
        df_Y[df_Y.columns[0]].astype(
            self.Y_data[df_Y.columns[0]].dtype
        )
        
        line_data = pd.concat(
            [df_X, df_Y],
            axis=1
        )

        # 构造线配置
        line_config = ElemConfig(
            elem_type = 'Line',
            params = deepcopy(line_data)
        )

        # 构造散点配置
        scatter_config = ElemConfig(
            elem_type = 'Scatter',
            params = deepcopy(scatter_data)
        )

        # 构造图表配置
        chart_config = ChartConfig(
            cell_type = CellType.CHART,
            cell_num = 2,
            elem_list = [
                line_config, 
                scatter_config
            ]
        )

        # 构造结果
        result = OpResult(
            text_list = [text_config],
            chart_list = [chart_config],
            data_list = None
        )

        return result

    # 检查方法
    def check(self, *args, **kwargs):

        # 检查X是否存在数据缺失值
        if self.X_data.isna().any().any():
            
           raise ValueError(
               '\n 线性回归要求没有缺失值的数据。' +
               '而 X 数据存在缺失值。'
               '\n Linear Regression requires no missing data.' + 
               'But X data has missing values.'
            )

        # 检查Y是否存在数据缺失值
        if self.Y_data.isna().any().any():

           raise ValueError(
                '\n 线性回归要求没有缺失值的数据。' +
                '而 Y 数据存在缺失值。' +
                '\n Linear Regression requires no missing data.' + 
                'But Y data has missing values.'
            )