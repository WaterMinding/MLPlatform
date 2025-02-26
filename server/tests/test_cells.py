# 导入系统包
import os
import sys

# 导入依赖包
import h5py
import asyncio
import pandas as pd
from copy import deepcopy
from pandas import DataFrame as DF


# 确定测试脚本路径
FILE_PATH = os.path.abspath(__file__)

# 确定待测试脚本路径
TO_TEST = FILE_PATH.replace(
    'tests\\test_cells.py', 
    ''
)

# 配置被测试包路径
sys.path.append(TO_TEST)

# 导入测试包
import unittest

# 导入待测试模块
from cells import TextCell 
from cells import OpCell
from cells import Element
from cells import ChartCell
from cells import Variable
from cells import DataCell

# 导入错误包
import warnings
from typeguard import TypeCheckError
from mlp_exception import ConstructionError
from mlp_exception import DataQueryError


# 创建文本块测试类
class TestTextCell(unittest.TestCase):

    # 测试文本块构造方法
    def test_init(self):

        # 输出测试信息
        print('\n测试文本块构造方法')

        # 构造文本配置字典
        text_config = {
            'cell_id': 'text_1',
            'text': 'Hello World!'
        }

        # 创建文本块对象
        text_cell = TextCell(text_config)

        # 检验文本块对象属性
        self.assertEqual(text_cell.get_cell_id(), 'text_1')
        self.assertEqual(text_cell.get_text(), 'Hello World!')

        # 构造类型错误的文本配置字典
        text_config = {
            'cell_id': b'text_1',
            'text': 123
        }

        # 检验类型错误
        with self.assertRaises(ConstructionError):

            # 创建文本块对象
            text_cell = TextCell(text_config)
        
        # 构造缺少必要参数的文本配置字典
        text_config = {
            'cell_id': 'text_1'
        }

        # 检验缺少必要参数
        with self.assertRaises(ConstructionError):

            # 创建文本块对象
            text_cell = TextCell(text_config)
        
        # 构造参数错误的文本配置字典
        text_config = {
            'cell_d': 'text_1',
            'text': 'Hello World!',
        }

        # 检验参数错误
        with self.assertRaises(ConstructionError):

            # 创建文本块对象
            text_cell = TextCell(text_config)

    # 测试获取文本块ID方法
    def test_get_cell_id(self):

        # 输出测试开始信息
        print('\n测试获取文本块ID方法')

        # 构造正确的文本配置字典
        text_config = {
            'cell_id': 'text_1',
            'text': 'Hello World!'
        }

        # 创建文本块对象
        text_cell = TextCell(text_config)

        # 检验文本块对象属性
        self.assertEqual(text_cell.get_cell_id(), 'text_1')
    
    # 测试生成文本配置字典方法
    def test_get_text_config(self):

        # 输出测试开始信息
        print('\n测试生成文本配置字典方法')

        # 构造正确的文本配置字典
        text_config = {
            'cell_id': 'text_1',
            'text': 'Hello World!'
        }

        # 创建文本块对象
        text_cell = TextCell(text_config)

        # 检验文本配置字典
        self.assertEqual(text_cell.get_config(), text_config)
    
    # 测试返回文本字符串方法
    def test_get_text(self):

        # 输出测试开始信息
        print('\n测试返回文本字符串方法')

        # 构造正确的文本配置字典
        text_config = {
            'cell_id': 'text_1',
            'text': 'Hello World!'
        }

        # 创建文本块对象
        text_cell = TextCell(text_config)
        
        # 检验文本字符串
        self.assertEqual(text_cell.get_text(), 'Hello World!')

    # 测试编辑文本字符串方法
    def test_edit_text(self):

        # 输出测试开始信息
        print('\n测试编辑文本字符串方法')

        # 构造正确的文本配置字典
        text_config = {
            'cell_id': 'text_1',
            'text': 'Hello World!'
        }

        # 创建文本块对象
        text_cell = TextCell(text_config)

        # 编辑文本字符串
        text_cell.edit_text('Hello Python!')

        # 检验文本字符串
        self.assertEqual(text_cell.get_text(), 'Hello Python!')


# 创建算子块测试类
class TestOpCell(unittest.TestCase):

    # 测试算子块构造方法
    def test_init(self):

        # 输出测试开始信息
        print('\n测试算子块构造方法')

        # 构造正确的算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2},
            'vars': {'result': 0}
        }

        # 创建算子块对象
        op_cell = OpCell(op_config)

        # 检验算子块属性
        self.assertEqual(op_cell.get_cell_id(), 'op_1')
        clones = ['add',{'1': 1, '2': 2}, {'result': 0}]
        self.assertEqual(op_cell.get_attributes_clone(), clones)

        # 构造类型错误的算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2},
            'vars': 'result'
        }

        # 检验类型错误
        with self.assertRaises(ConstructionError):
            OpCell(op_config)

        # 构造缺少属性算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2}
        }

        # 检验缺少属性
        with self.assertRaises(ConstructionError):
            OpCell(op_config)
        
        # 构造参数错误的算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_tpe': 'add',
            'params': {'1': 1, '2': 2},
            'var': {'result': 0}
        }

        # 检验参数错误
        with self.assertRaises(ConstructionError):
            OpCell(op_config)

    # 测试获取算子块ID方法
    def test_get_cell_id(self):

        # 输出测试开始信息
        print('\n测试获取算子块ID方法')

        # 构造正确的算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2},
            'vars': {'result': 0}
        }

        # 构造算子块
        op_cell = OpCell(op_config)

        # 获取算子块ID
        cell_id = op_cell.get_cell_id()

        # 检验算子块ID是否正确
        self.assertEqual(cell_id, 'op_1')

    # 测试生成算子配置字典方法
    def test_get_config(self):

        # 输出测试开始信息
        print('\n测试生成算子配置字典方法')

        # 构造正确的算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2},
            'vars': {'result': 0}
        }

        # 构造算子块
        op_cell = OpCell(op_config)

        # 生成算子配置字典
        config = op_cell.get_config()

        # 检验算子配置字典是否正确
        self.assertEqual(config, op_config)

    # 测试生成属性克隆列表方法
    def test_get_attributes_clone(self):

        # 输出测试开始信息
        print('\n测试生成属性克隆列表方法')

        # 构造正确的算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2},
            'vars': {'result': 0}
        }

        # 构造算子块
        op_cell = OpCell(op_config)

        # 生成属性克隆列表
        attributes_clone = op_cell.get_attributes_clone()

        # 检验属性克隆列表是否正确
        self.assertEqual(
            attributes_clone, 
            ['add', {'1': 1, '2': 2}, {'result': 0}]
        )
    
    # 测试编辑参数或变量字典方法
    def test_edit_content(self):

        # 输出测试开始信息
        print('\n测试编辑参数或变量字典方法')

        # 构造正确的算子配置字典
        op_config = {
            'cell_id': 'op_1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2},
            'vars': {'result': 0}
        }

        # 构造算子块
        op_cell = OpCell(op_config)

        # 编辑参数与变量字典
        op_cell.edit_content(
            to_edit = 'params',
            new_content = {'1': 1, '2': 3}
        )

        op_cell.edit_content(
            to_edit = 'vars',
            new_content = {'result': 3}
        )

        # 检验参数与变量字典是否正确
        self.assertEqual(
            op_cell.get_config()['params'],
            {'1': 1, '2': 3}
        )

        self.assertEqual(
            op_cell.get_config()['vars'],
            {'result': 3}
        )


# 创建元素测试类
class TestElement(unittest.TestCase):

    # 测试元素类构造方法
    def test_init(self):

        # 输出测试开始信息
        print('\n测试元素类构造方法')

        # 构造元素对象(传入正确参数)
        element = Element(
            elem_type = 'bar',
            docu_inner_path = '/group/element'
        )

        # 检验元素属性是否正确
        self.assertEqual(

            asyncio.run(
                element.get_elem_config(
                    with_data = False,
                    docu_path = None                    
                ),
            ),

            {
                'elem_type': 'bar',
                'elem_data': '/group/element'
            }
        )

        # 构造元素对象(传入错误参数)
        with self.assertRaises(TypeCheckError):
            Element(
                elem_type = 'bar',
                docu_inner_path = 123
            )

    # 测试生成元素配置字典方法与读取元素数据方法
    def test_get_elem_config(self):

        # 输出测试开始信息
        print('\n测试生成元素配置字典方法')

        global FILE_PATH

        # 测试文件路径
        test_path = FILE_PATH.replace('test_cells.py','example.h5')

        # 构造HDF5文件
        with h5py.File(test_path, 'w') as f:
            pass

        # 保存数据
        with pd.HDFStore(test_path, 'w') as store:
            store['/group/element'] = DF([1,2,3])

        # 构造元素对象
        element = Element(
            elem_type = 'bar',
            docu_inner_path = '/group/element'
        )

        # 获取元素配置字典
        config_dict = asyncio.run(
            element.get_elem_config(
                with_data = True,
                docu_path = test_path
            )
        )

        # 检查元素配置字典中elem_type是否正确
        self.assertEqual(config_dict['elem_type'], 'bar')

        # 检查元素配置字典中elem_data是否正确
        self.assertTrue(config_dict['elem_data'].equals(DF([1,2,3])))

        # 生成元素配置字典时传入错误参数
        with self.assertRaises(TypeCheckError):
            
            asyncio.run(
                element.get_elem_config(
                    with_data = 123,
                    docu_path = test_path
                )
            )
        
        # 删除测试文件
        os.remove(test_path)


# 创建图像块测试类
class TestChartCell(unittest.TestCase):

    # 测试图像块所有方法
    def test_init(self):

        # 输出测试开始信息
        print('\n测试图像块所有方法')

        global FILE_PATH

        # 测试文件路径
        test_path = FILE_PATH.replace('test_cells.py','example_chart.h5')

        # 构造HDF5文件
        with h5py.File(test_path, 'w') as f:
            f.create_group('Chart')

        # 构造元素数据类型为str的图像配置字典
        config_dict = {
            'cell_id': 'chart12138',
            'elem_data_type': 'str',
            'elem_config_list':[{
                'elem_type': 'bar',
                'elem_data': '/group/element'
            }],
        }

        # 构造图像块
        chart_cell = ChartCell(config_dict, test_path)

        # 生成图像配置字典
        config_dict_new = asyncio.run(
            chart_cell.get_config(
                with_data = False
            )
        )

        # 检查图像配置字典
        self.assertEqual(
            config_dict, 
            config_dict_new
        )

        # 构造元素数据类型为DF的图像配置字典
        config_dict = {
            'cell_id': 'chart12138',
            'elem_data_type': 'DF',
            'elem_config_list':[{
                'elem_type': 'bar',
                'elem_data': DF([1,2,3])
            },
            {
                'elem_type': 'bar',
                'elem_data': DF([4,5,6])
            }]
        }

        # 构造图像块
        chart_cell = ChartCell(config_dict, test_path)

        # 生成图像配置字典
        config_dict_new = asyncio.run(
            chart_cell.get_config(
                with_data = True
            )
        )

        # 检查图像配置字典中的数据
        self.assertTrue(
            config_dict['elem_config_list'][0]['elem_data'].equals(
                config_dict_new['elem_config_list'][0]['elem_data']
            ) 
            
            and

            config_dict['elem_config_list'][1]['elem_data'].equals(
                config_dict_new['elem_config_list'][1]['elem_data']
            )
        )

        # 测试构造方法异常抛出
        config_dict['elem_data_type'] = 'int'
        with self.assertRaises(ConstructionError):
            chart_cell = ChartCell(config_dict, test_path)
        
        with self.assertRaises(TypeCheckError):
            chart_cell = ChartCell(config_dict, 18)
        
        # 测试 get_config 方法异常抛出
        with self.assertRaises(TypeCheckError):
            asyncio.run(
                chart_cell.get_config(
                    with_data = 'True'
                )
            )
        
        # 测试获取图像块ID方法
        self.assertEqual(
            chart_cell.get_cell_id(),
            'chart12138'
        )

        # 删除测试文件
        os.remove(test_path)


# 创建变量测试类
class TestVariable(unittest.TestCase):

    # 测试变量类构造方法
    def test_init(self):

        # 输出测试开始信息
        print('\n测试变量类构造方法')

        # 构造正确的变量字符串
        var_str = 'var_name:Numerical:data12138'

        # 构造变量对象
        var_cell = Variable(var_str)
        
        # 验证变量对象属性
        self.assertEqual(
            var_cell.var_name,
            'var_name'
        )

        self.assertEqual(
            var_cell.var_type,
            'Numerical'
        )

        self.assertEqual(
            var_cell.owner_id,
            'data12138'
        )

        # 测试构造方法异常抛出
        with self.assertRaises(ConstructionError):
            var_cell = Variable('var_name:Numerical')

        with self.assertRaises(TypeCheckError):
            var_cell = Variable(999)

    # 测试生成变量字符串方法
    def test_get_var_str(self):

        # 输出测试开始信息
        print('\n测试变量类生成变量字符串方法')

        # 构造变量对象
        var_cell = Variable('var_name:Numerical:data12138')

        # 验证变量字符串
        self.assertEqual(
            var_cell.get_var_str(),
            'var_name:Numerical:data12138'
        )


# 创建数据块测试类
class TestDataCell(unittest.TestCase):

    # 测试数据块构造方法与生成数据配置字典方法与获取数据块ID方法
    def test_init(self):

        # 输出测试开始信息
        print('''
            \n测试数据块类构造方法
            \n与生成数据配置字典方法
            \n与获取数据块ID方法
        ''')

        # 获取测试脚本位置
        global FILE_PATH

        # 构造测试文件路径
        test_path = FILE_PATH.replace(
            'test_cells.py',
            'example_data.h5'
        )
        
        # 构造数据配置字典
        data_config = {
            "cell_id": "data12138",
            "var_str_list": [
                'var_name:Numerical:data12138',
                'var_name2:Categorical:data12138'
            ]
        }
        
        # 构造数据块对象
        data_cell = DataCell(data_config, test_path)

        # 生成数据配置字典方法
        data_config_new = data_cell.get_config()

        # 验证数据配置字典
        self.assertEqual(
            data_config_new,
            data_config
        )
    
        # 构造存在错误的数据配置字典
        data_config_err = {
            "cell_id": "data12138",
            "var_str_list":[12138]
        }

        # 构造数据块对象
        with self.assertRaises(ConstructionError):
            data_cell = DataCell(data_config_err, test_path)
        
        # 验证TypeCheckError
        with self.assertRaises(TypeCheckError):
            data_cell = DataCell(data_config, 123)
        
        # 测试返回数据块ID方法
        self.assertEqual(
            data_cell.get_cell_id(),
            data_config['cell_id']
        )
        
    # 测试数据块列追加数据方法
    def test_append_data(self):

        # 输出测试开始信息
        print('''
            \n测试数据块数据查询方法
            \n与测试数据块数据替换方法  
            \n与测试数据块列追加数据方法  
        ''')

        # 获取测试脚本位置
        global FILE_PATH

        # 构造测试文件路径
        test_path = FILE_PATH.replace(
            'test_cells.py',
            'example_data.h5'
        )

        # 创建测试文件
        with h5py.File(test_path, 'w') as f:
            f.create_group('Data')

        # 构造数据配置字典
        data_config = {
            "cell_id": "data12138",
            "var_str_list": [
                'var_name:Numerical:data12138',
            ]
        }

        # 创建数据
        data = pd.DataFrame({
            'var_name': [1, 2, 3]
        })

        # 保存数据
        with pd.HDFStore(test_path) as store:

            store['/Data/data12138'] = data
        
        # 构造数据块对象
        data_cell = DataCell(data_config, test_path)

        # 测试数据块数据查询方法
        # 查询正确数据
        self.assertTrue(
            data['var_name'].equals(
                asyncio.run(
                    data_cell.query_data(
                        'var_name:Numerical:data12138'
                    )
                ).df_register
            )
        )

        # 查询错误数据
        with self.assertRaises(DataQueryError):    
            asyncio.run(
                data_cell.query_data(
                    'var_nadasme:Numerical:data12138123'
                )
            )
        
        # 测试数据块数据替换方法
        # 构造变量对象
        var_obj = Variable('var_name:Numerical:data12138')
        var_obj.df_register = pd.DataFrame({
            'var_name': [4, 5, 6]
        })
        var_obj.usage = 'replace'

        # 替换数据
        asyncio.run(
            data_cell.replace_data(deepcopy([var_obj]))
        )

        # 查询替换后的数据
        self.assertTrue(
            var_obj.df_register['var_name'].equals(
                asyncio.run(
                    data_cell.query_data(
                        'var_name:Numerical:data12138'
                    )
                ).df_register
            )
        )

        # 传入错误参数类型
        with self.assertRaises(TypeCheckError):
            asyncio.run(
                data_cell.replace_data(1)
            )
        
        # 传入错误变量
        var_obj.var_name = 'hhh'
        var_obj.df_register = pd.DataFrame({
            'hhh': [4, 5, 6]
        })
        with self.assertRaises(DataQueryError):
            asyncio.run(
                data_cell.replace_data(deepcopy([var_obj]))
            )
        
        # 测试数据块列追加数据方法
        # 追加数据
        asyncio.run(
            data_cell.append_data(
                [deepcopy(var_obj)]
            )
        )

        # 查询追加后的数据
        self.assertTrue(
            var_obj.df_register['hhh'].equals(
                asyncio.run(
                    data_cell.query_data(
                        'hhh:Numerical:data12138'
                    )
                ).df_register
            )
        )
        
        # 传入错误参数类型
        with self.assertRaises(TypeCheckError):
            asyncio.run(
                data_cell.append_data(1)
            )

        # 传入重复变量(无(n))
        asyncio.run(
            data_cell.append_data(
                [deepcopy(var_obj)]
            )
        )

        # 查询追加后的数据
        self.assertTrue(
            var_obj.df_register['hhh'].equals(
                asyncio.run(
                    data_cell.query_data(
                        'hhh(1):Numerical:data12138'
                    )
                ).df_register
            )
        )

        # 传入重复变量(有(n))
        var_obj.var_name = 'hhh(1)'
        var_obj.df_register = pd.DataFrame({
            'hhh(1)': [4, 5, 6]
        })
        
        # 追加数据
        asyncio.run(
            data_cell.append_data(
                [deepcopy(var_obj)]
            )
        )

        # 查询追加后的数据
        self.assertTrue(
            var_obj.df_register['hhh(1)'].equals(
                asyncio.run(
                    data_cell.query_data(
                        'hhh(2):Numerical:data12138'
                    )
                ).df_register
            )
        )

        # 追加数据
        asyncio.run(
            data_cell.append_data(
                [deepcopy(var_obj)]
            )
        )

        # 查询追加后的数据
        self.assertTrue(
            var_obj.df_register['hhh(1)'].equals(
                asyncio.run(
                    data_cell.query_data(
                        'hhh(3):Numerical:data12138'
                    )
                ).df_register
            )
        )

        # 删除测试文件
        os.remove('example_data.h5')