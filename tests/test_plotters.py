# 导入标准库模块
import os
import sys
import base64
import unittest
from io import BytesIO
from copy import deepcopy

# 导入第三方库模块
from PIL import Image
from pandas import DataFrame as DF
from matplotlib import pyplot as plt
from typeguard import TypeCheckError


# 确定脚本位置
FILE_PATH = os.path.abspath(__file__)

# 确定项目根目录
TESTS = os.path.dirname(FILE_PATH)
ROOT = os.path.dirname(TESTS)

# 添加项目根目录到系统路径
sys.path.append(ROOT)


# 导入自定义模块
from app import plotter_entry
from app import PlotterNotFoundError


# 测试绘图层
class Test_plotter_entry(unittest.TestCase):

    # 测试类初始化方法
    def setUp(self):
        
        # 确定绘图层路径
        PLOT_PATH = f'{ROOT}/app/plotters/'
        
        # 在绘图层中注册测试用绘图器
        with open(
            f'{PLOT_PATH}/test_plotter.py', 'w',
            encoding = 'utf-8'
        ) as script:
            
            # 写入测试用绘图脚本
            script.write(
                'class TestPlotter:\n    pass'
            )
        
        # 在绘图层配置文件中注册测试用绘图器
        with open(
            f'{PLOT_PATH}/plotter_config.toml', 'a',
            encoding = 'utf-8'
        ) as config:

            # 写入测试用绘图器配置
            config.write(
                'TestPlotter = "test_plotter.TestPlotter"'
            )


    # 测试类清理方法
    def tearDown(self):
        
        # 确定绘图层路径
        PLOT_PATH = f'{ROOT}/app/plotters/'

        # 删除测试用绘图脚本
        os.remove(f'{PLOT_PATH}/test_plotter.py')

        # 删除测试用绘图配置
        with open(
            f'{PLOT_PATH}/plotter_config.toml', 'r',
            encoding = 'utf-8'
        ) as config:
            
            # 读取绘图层配置文件
            config_lines = config.readlines()

        with open(
            f'{PLOT_PATH}/plotter_config.toml', 'w',
            encoding = 'utf-8'
        ) as config:

            # 重新写入绘图层配置文件
            for line in config_lines:

                # 如果当前行不是测试用绘图配置
                if 'TestPlotter' not in line:
                    config.write(line)

    # 测试绘图层入口函数
    def test_plotter_entry(self):

        # 输出测试信息
        print("\n测试绘图层：绘图入口函数")

        # 测试typechecked装饰器
        with self.assertRaises(TypeCheckError):
            plotter_entry(123)

        # 测试PlotterNotFoundError
        with self.assertRaises(
            PlotterNotFoundError
        ):
            plotter_entry(
                {
                    'cell_num': 1,
                    'elem_list': [{
                        'elem_type': '123',
                        'params': DF(
                            {'x':[1,2,3]}
                        )
                    }]
                }
            )

        # 测试绘图类不符合协议引发的异常
        with self.assertRaises(TypeError):
            plotter_entry(
                {
                    'cell_num': 1,
                    'elem_list': [{
                        'elem_type': 'TestPlotter',
                        'params': DF(
                            {'x':[1,2,3]}
                        )
                    }]
                }
            )
    
    # 测试直线绘图器
    def test_line(self):

        # 输出测试信息
        print("\n测试绘图层：直线绘图器")

        # 构造图像配置字典
        config = {
            'cell_num': 1,
            'elem_list': [{
                'elem_type': 'Line',
                'params': DF({
                    'x': [1, 2, 3],
                    'y': [1, 2, 3],
                })
            }]
        }

        # 构造错误的图像配置字典
        config_error = deepcopy(config)
        config_error['elem_list'][0]['params'] = DF({
            'x': [1, 2, 3],
        })

        # 传入错误参数
        with self.assertRaises(TypeError):
            plotter_entry(config_error)

        # 调用绘图器入口函数
        result = plotter_entry(config)

        # 展示绘图结果
        image_data = base64.b64decode(result)
        with Image.open(BytesIO(image_data)) as img:
            plt.imshow(img)
            plt.axis('off')
            plt.show()
            plt.close()


    # 测试散点绘图器
    def test_scatter(self):

        # 输出测试信息
        print("\n测试绘图层：散点绘图器")

        # 构造图像配置字典
        config = {
            'cell_num': 1,
            'elem_list': [{
                'elem_type': 'Scatter',
                'params': DF({
                    'x': [1, 2, 3],
                    'y': [1, 2, 3],
                })
            }]
        }

        # 构造错误的图像配置字典
        config_error = deepcopy(config)
        config_error['elem_list'][0]['params'] = DF({
            'x': [1, 2, 3],
        })

        # 传入错误参数
        with self.assertRaises(TypeError):
            plotter_entry(config_error)

        # 调用绘图器入口函数
        result = plotter_entry(config)

        # 展示绘图结果
        image_data = base64.b64decode(result)
        with Image.open(BytesIO(image_data)) as img:
            plt.imshow(img)
            plt.axis('off')
            plt.show()
            plt.close()
    
    # 测试绘图器协同
    def test_plotter_collaboration(self):

        # 输出测试信息
        print("\n测试绘图层：绘图器协同")

        # 构造图像配置字典
        config = {
            'cell_num': 1,
            'elem_list': [
                {
                    'elem_type': 'Scatter',
                    'params': DF({
                        'x': [1, 2, 3],
                        'y': [1, 2, 3],
                    })
                },
                {
                    'elem_type': 'Line',
                    'params': DF({
                        'x': [1, 2, 3],
                        'y': [1, 2, 3],
                    })
                }
            ]
        }

        # 调用绘图器入口函数
        result = plotter_entry(config)

        # 展示绘图结果
        image_data = base64.b64decode(result)
        with Image.open(BytesIO(image_data)) as img:
            plt.imshow(img)
            plt.axis('off')
            plt.show()
            plt.close()

