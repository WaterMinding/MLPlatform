# 导入标准库模块
import os
import base64
import tomllib
import importlib
from io import BytesIO

# 导入第三方库模块
import matplotlib
from typeguard import typechecked
from matplotlib.figure import Figure

# 导入自定义模块
from .protocols import Plotter
from ..operators import ChartConfig
from ..mlp_exceptions import PlotterNotFoundError


# 获取当前脚本路径
FILE_PATH = os.path.abspath(__file__)

# 确定配置文件路径
PLOTTER_CONFIG_PATH = os.path.dirname(
    FILE_PATH
) + '/plotter_config.toml'


# 绘图层入口函数
# 参数1：chart_config - 绘图配置字典
@typechecked
def plotter_entry(chart_config: ChartConfig):

    # 读取配置文件
    with open(
        PLOTTER_CONFIG_PATH, 
        mode = 'rb'
    ) as file:
        
        plotter_config = tomllib.load(file)
    
    # 获取绘图目录
    plotter_dir = plotter_config['plotter_dict']

    # 创建Figure对象
    fig = Figure()

    # 创建Axes对象
    ax = fig.add_subplot()

    # 分析图表配置
    elem_list = chart_config['elem_list']

    # 调用绘图函数
    for elem_config in elem_list:

        # 检查元素类型是否存在于绘图目录
        if elem_config['elem_type'] not in plotter_dir:

            # 抛出异常
            raise PlotterNotFoundError(
                f'{elem_config['elem_type']}'
            )

        # 获取绘图类
        plotter = plotter_dir[elem_config['elem_type']]
        modu, clss = plotter.split('.')
        modu = importlib.import_module(
            name = '.' + modu,
            package = 'app.plotters'
        )
        clss = getattr(modu, clss)

        # 检查绘图类是否符合协议
        if not issubclass(clss, Plotter):

            # 抛出异常
            raise TypeError(
                '绘图类必须符合绘图类型协议\n' +
                'Operator class must be subclass of Operator\
                 which is a Protocol class'
            )

        # 创建绘图对象
        plotter = clss(elem_config)
        
        # 调用绘图方法
        plotter.plot(ax)

    # 构造缓冲区
    buffer = BytesIO()

    # 保存图像到缓冲区
    fig.savefig(
        buffer, 
        format = 'png',
        bbox_inches='tight'
    )

    # 读取缓冲区内容
    buffer.seek(0)
    image_bytes = buffer.read()
    
    # 将图像转换为base64编码
    result = base64.b64encode(
        image_bytes
    ).decode('utf-8')

    # 返回结果
    return result
        