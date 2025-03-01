# 导入标准库
import os
import sys
import asyncio

# 确定测试脚本路径
FILE_PATH = os.path.abspath(__file__)

# 确定待测试脚本路径
TO_TEST = FILE_PATH.replace(
    'tests\\test_cells.py', 
    ''
)

# 配置被测试包路径
sys.path.append(TO_TEST)

# 导入第三方库
import h5py
import unittest

# 导入待测试模块
from docu import Docu
from mlp_exception import FilePathError
from mlp_exception import ConstructionError

# 测试用文档
test_docu = None


# 创建文档类测试类
class TestDocu(unittest.TestCase):

    # 测试文档类构造方法
    def test_init(self):

        # 输出测试信息
        print(
            '\n测试文档类构造方法'
        )

        # 以“新建”模式构造文档类对象
        docu = Docu(
            docu_name = 'test_docu',
            mode = 'new',
            docu_path = None
        )

        # 获取数据缓存区路径
        cache_path = FILE_PATH.replace(
            'tests\\test_docu.py',
            'data_cache'
        )

        # 检查文档文件是否正确构造
        with h5py.File(
            f'{cache_path}/test.docu'
        ) as docu_file:
            
            # 检查文件内部是否包含图像组
            self.assertIn(
                'Chart',
                docu_file.keys()
            )

            # 检查文件内部是否包含数据组
            self.assertIn(
                'Data',
                docu_file.keys()
            )

        # 获取文档配置字典
        docu_config = asyncio.run(docu.get_config())

        # 获取文档文件路径
        docu_path = docu.get_docu_path()

        # 检查是否保存了文档名和文档路径
        self.assertEqual(
            docu_config['docu_name'],
            'test_docu'
        )
        
        self.assertTrue(
            docu_path != None
        )

        # 以“打开”模式构造文档类对象
        docu = Docu(
            docu_name = 'test_docu',
            docu_path = f'{cache_path}/test.docu',
            mode = 'open',
        )

        # 获取文档配置字典
        docu_config = asyncio.run(docu.get_config())

        # 获取文档文件路径
        docu_path = docu.get_docu_path()

        # 检查是否保存了文档名和文档路径
        self.assertEqual(
            docu_config['docu_name'],
            'test_docu'
        )

        self.assertEqual(
            docu_path,
            f'{cache_path}/test.docu'
        )

        # 以“打开”模式构造文档类对象，但扩展名不正确
        with self.assertRaises(FilePathError):
            docu = Docu(
                docu_name = 'test_docu',
                docu_path = f'{cache_path}/test_cells.py',
                mode = 'open',
            )
        
        # 以“打开”模式构造文档类对象，但文件不存在
        with self.assertRaises(FilePathError):
            docu = Docu(
                docu_name = 'test_docu',
                docu_path = f'{cache_path}/test_c.docu',
                mode = 'open',
            )
        
        # 以错误模式构造文档类对象
        with self.assertRaises(ConstructionError):
            docu = Docu(
                docu_name = 'test_docu',
                docu_path = f'{cache_path}/test.docu',
                mode = 'error',
            )
        
        # 保存文档对象引用
        test_docu = docu

    # 测试添加与获取块方法
    def test_get_cell(self):

        # 输出测试开始信息
        print('''
            \n测试文档类添加块方法
            \n与文档类获取块方法
        ''')

        # 打开文档
        docu:Docu = test_docu

        # 获取文档名
        docu_name = docu.get_docu_name()
        
        # 构造数据配置字典
        data_config = {
            'cell_id': f'data:{docu_name}:1',
            'var_str_list': [
                'var_name:Numerical:data12138'
            ]
        }

        # 添加数据块
        docu.add_data_cell(data_config)

        # 构造文本配置字典
        text_config = {
            'cell_id': f'text:{docu_name}:1',
            'text': 'test_text'
        }

        # 添加文本块
        docu.add_edit_area_cell(text_config, 'text')

        # 构造算子配置字典
        operator_config = {
            'cell_id': f'op:{docu_name}:1',
            'op_type': 'add',
            'params': {'1': 1, '2': 2},
            'vars': {'result': 0}
        }

        # 添加算子块
        docu.add_edit_area_cell(operator_config, 'operator')

        # 构造图像配置字典
        chart_config = {
            'cell_id': f'chart:{docu_name}:1',
            'elem_data_type': 'str',
            'elem_config_list':[]
        }

        # 添加图像块
        docu.add_edit_area_cell(chart_config, 'chart')

        # 获取数据块




            




