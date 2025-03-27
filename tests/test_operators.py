# 导入标准库模块
import os
import sys
import unittest

# 导入第三方库模块
import pandas as pd
from pandas import DataFrame as DF
from typeguard import TypeCheckError


# 确定脚本位置
FILE_PATH = os.path.abspath(__file__)

# 确定项目根目录
TESTS = os.path.dirname(FILE_PATH)
ROOT = os.path.dirname(TESTS)

# 添加项目根目录到系统路径
sys.path.append(ROOT)


# 导入自定义模块
from app import Variable
from app import op_entry
from app import OperatorNotFoundError
from app .protocols import CellType
from app.protocols import OpConfig


# 测试算子层
class Test_op_entry(unittest.TestCase):

    # 测试类初始化方法
    def setUp(self):

        # 确定算子层路径
        OP_PATH = f'{ROOT}/app/operators/'
        
        # 在算子层中注册测试用算子
        with open(
            f'{OP_PATH}/test_op.py', 'w',
            encoding = 'utf-8'
        ) as script:
            
            # 写入测试用算子脚本
            script.write(
                'class TestOp:\n    pass'
            )
        
        # 在算子层配置文件中注册测试用算子
        with open(
            f'{OP_PATH}/op_config.toml', 'a',
            encoding = 'utf-8'
        ) as config:

            # 写入测试用算子配置
            config.write(
                'TestOp = "test_op.TestOp"'
            )
        
    # 测试类清理方法
    def tearDown(self):

        # 确定算子层路径
        OP_PATH = f'{ROOT}/app/operators/'

        # 删除测试用算子脚本
        os.remove(f'{OP_PATH}/test_op.py')

        # 删除测试用算子配置
        with open(
            f'{OP_PATH}/op_config.toml', 'r',
            encoding = 'utf-8'
        ) as config:
            
            # 读取算子层配置文件
            config_lines = config.readlines()

        with open(
            f'{OP_PATH}/op_config.toml', 'w',
            encoding = 'utf-8'
        ) as config:

            # 重新写入算子层配置文件
            for line in config_lines:

                # 如果当前行不是测试用算子配置
                if 'TestOp' not in line:
                    config.write(line)
    
    # 测试算子层入口函数
    def test_op_entry(self):

        # 输出测试信息
        print('\n测试算子层：算子入口函数')

        # 测试typechecked装饰器
        with self.assertRaises(TypeCheckError):
            op_entry(123),
        
        # 测试OperatorNotFoundError
        with self.assertRaises(OperatorNotFoundError):
            op_entry(
                OpConfig(
                    cell_type = CellType.OP,
                    op_name = 'wuliwalajiliguala',
                    parameters = None, 
                    variables = None
                )
            ),

        # 测试算子类不符合协议引发的TypeError
        with self.assertRaises(TypeError):

            op_entry(
                OpConfig(
                    cell_type = CellType.OP,
                    op_name = 'TestOp', 
                    parameters =  None, 
                    variables = None
                )
            )
        
    # 测试线性回归算子
    def test_linear_regression(self):

        # 输出测试信息
        print('\n测试算子层：线性回归算子')

        # 构造测试变量
        var_1 = Variable('var1:quan:data12138')
        var_2 = Variable('var2:quan:data12138')
        var_1.register = DF({'var1': [1, 2, 3, 4, 5]})
        var_2.register = DF({'var2': [1, 2, 3, 4, 5]})

        # 构造测试变量字典
        var_dict = {'X': var_1, 'Y': var_2}

        # 测试线性回归算子
        results= op_entry(
            OpConfig(
                cell_type = CellType.OP,
                op_name = 'LinearRegression', 
                parameters = None, 
                variables = var_dict
            )
        )

        # 构造正确图像数据
        correct_chart = DF({
            'var1': [1, 2, 3, 4, 5],
            'var2': [1, 2, 3, 4, 5]
        })

        # 获取文本结果
        text = results.text_list

        # 获取图像结果
        chart = results.chart_list

        # 获取数据结果
        data = results.data_list

        # 检查输出的文本结果是否正确
        self.assertEqual(
            text[0].cell_num,
            1
        )

        self.assertIsInstance(
            text[0].text,
            str
        )

        # 检查输出的图像结果是否正确
        self.assertEqual(
            chart[0].cell_num,
            2
        )

        self.assertEqual(
            chart[0].elem_list[0].elem_type,
            'Line'            
        )

        self.assertTrue(
            correct_chart.equals(
                chart[0].elem_list[0].params
            )
        )

        self.assertEqual(
            chart[0].elem_list[1].elem_type,
            'Scatter'
        )

        self.assertTrue(
            correct_chart.equals(
                chart[0].elem_list[1].params
            )
        )

        # 检查输出的数据结果是否正确
        self.assertEqual(data, None)

        # 输出结果
        #print(results)