# 导入第三方库模块
import matplotlib
from matplotlib.axes import Axes 
from typeguard import typechecked
from pandas import DataFrame as DF

# 导入自定义模块
from ..protocols import ElemConfig

# 设置中文和负号输出
matplotlib.rcParams['font.family'] = 'SimHei'
matplotlib.rcParams['axes.unicode_minus'] = False 


# 直线类
class Line:

    # 构造方法
    # 参数1：elem_config - 元素配置
    def __init__(
        self,
        elem_config: ElemConfig,
    ):
        
        # 保存参数
        self.params: DF = elem_config.params

        # 检查参数
        try:
            self.check_params()
        except Exception as e:
            raise e

    # 绘图方法
    # 参数1：ax - 子图对象
    def plot(self, ax: Axes):

        # 设置坐标轴标签
        ax.set_xlabel(
            self.params.columns[0]
        )
        ax.set_ylabel(
            self.params.columns[1]
        )

        # 绘制直线
        ax.plot(
            self.params[
                self.params.columns[0]
            ],
            self.params[
                self.params.columns[1]
            ],
            color = 'orange',
            linewidth = 3
        )

        # 显示图例
        #ax.legend()

    # 检查参数方法
    def check_params(self):

        # 检查参数列数
        if len(self.params.columns) < 2:
            
            raise TypeError(
                '绘图参数列数不足，至少需要2列\n' + 
                'There is supposed to be at least +'
                '2 columns for plotting'
            )
        
