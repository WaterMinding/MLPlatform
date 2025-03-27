# 导入标准库模块
import os
import sys
from copy import deepcopy

# 导入第三方库模块
import unittest
from duckdb import connect
from typeguard import TypeCheckError

# 确定脚本位置
FILE_PATH = os.path.abspath(__file__)

# 确定项目根目录
TESTS = os.path.dirname(FILE_PATH)
ROOT = os.path.dirname(TESTS)
print(ROOT)

# 添加项目根目录到系统路径
sys.path.append(ROOT)

# 导入自定义库模块
from app import DataPool
from app import ConstructionError
from app.protocols import DataConfig

# 测试运行时数据池类
class TestDataPool(unittest.TestCase):

    # 测试类初始化方法
    def setUp(self):
        
        # 构造数据池配置
        self.pool_config = {
            
            'data_12138': DataConfig(
                cell_id = 'data_12138',
                cell_name = 'data_12138',
                var_str_list = [
                    'var1:quan:data_12138',
                    'var2:quan:data_12138',
                    'var3:quan:data_12138'
                ]
            ),

            'data_12345': DataConfig(
                cell_id = 'data_12345',
                cell_name = 'data_12345',
                var_str_list = [
                    'var1:quan:data_12345',
                    'var2:quan:data_12345',
                    'var3:quan:data_12345'
                ]    
            )
        }

        # 定义数据池文件路径
        self.pool_path = f'{TESTS}/duck.db'

        # 定义数据池元信息表名
        self.meta_name = 'META_TABLE'

        # 创建测试用数据池文件
        with connect(f'{TESTS}/duck.db') as conn:

            conn.sql(
                f"CREATE TABLE {self.meta_name} " + 
                f"(cell_id VARCHAR, cell_name VARCHAR, " + 
                F"variables TEXT)"
            )
        

    # 测试类清理方法
    def tearDown(self):

        # 删除测试用数据池文件
        os.remove(self.pool_path)

    # 测试构造方法
    def test_init(self):

        # 输出测试信息
        print(
            "\
            \n测试运行时数据池类：构造方法 \
            \n测试运行时数据池类：获取数据池文件路径方法 \
            \n测试运行时数据池类：获取数据块字典方法 \
            "
        )

        # 测试typechecked装饰器
        with self.assertRaises(TypeCheckError):

            # 传入错误类型参数
            DataPool('dasdas','dasdas','dasdas')
        
        # 测试文件路径检查
        with self.assertRaises(FileNotFoundError):

            # 传入错误路径
            DataPool('dasdas',self.meta_name,self.pool_config)
        
        # 构造错误的数据池配置
        error_pool_config = deepcopy(self.pool_config)
        error_pool_config['data_12345'].var_str_list = 'dasdas'

        # 测试数据池构造异常
        with self.assertRaises(ConstructionError):

            # 传入错误的数据池配置
            DataPool(self.pool_path,self.meta_name,error_pool_config)
        
        # 传入正确的数据池配置
        pool = DataPool(self.pool_path,self.meta_name,self.pool_config)

        # 测试数据池文件路径
        self.assertEqual(pool.pool_path,self.pool_path)

        # 获取数据块字典
        cell_dict = pool.cell_dict

        # 测试数据块
        data_cell_1 = cell_dict['data_12138']
        data_cell_2 = cell_dict['data_12345']

        # 测试数据块ID
        self.assertEqual(
            data_cell_1.cell_id,
            'data_12138'
        )

        self.assertEqual(
            data_cell_2.cell_id,
            'data_12345'
        )

        # 测试数据块中数据池文件路径
        self.assertEqual(
            data_cell_1.pool_path,
            self.pool_path
        )

        self.assertEqual(
            data_cell_2.pool_path,
            self.pool_path
        )

        # 测试数据块中字符串列表
        self.assertEqual(
            data_cell_1.var_str_list,
            self.pool_config[
                'data_12138'
            ].var_str_list
        )

        self.assertEqual(
            data_cell_2.var_str_list,
            self.pool_config[
                'data_12345'
            ].var_str_list
        )

    # 测试添加数据块与获取数据块方法
    def test_add_cell(self):

        # 输出测试信息
        print(
            "\
            \n测试运行时数据池类：添加数据块方法 \
            \n测试运行时数据池类：获取数据块方法 \
            "
        )

        pool = DataPool(
            self.pool_path,
            self.meta_name,
            self.pool_config
        )

        # 测试添加数据块
        pool.add_cell(
            data_config = DataConfig(
                cell_id = 'data_12149',
                cell_name = 'data_12149',
                var_str_list = [
                    'var_1:cate:data_12149'
                ]
            )
        )

        # 获取数据块
        data_cell = pool.get_cell(
            cell_id = 'data_12149'
        )

        # 测试数据块中数据池文件路径
        self.assertEqual(
            data_cell.pool_path,
            self.pool_path
        )

        # 测试数据块ID
        self.assertEqual(
            data_cell.cell_id,
            'data_12149'
        )
        
        # 测试数据块中字符串列表
        self.assertEqual(
            data_cell.var_str_list,
            ['var_1:cate:data_12149']
        )

    # 测试获取数据池配置方法
    def test_get_config(self):

        # 输出测试信息
        print(
            "\n测试运行时数据池类：获取数据池配置方法"
        )

        # 构造数据池
        pool = DataPool(
            self.pool_path,
            self.meta_name,
            self.pool_config
        )

        # 获取数据池配置
        config = pool.get_config()

        # 测试数据池配置
        self.assertEqual(
            config,
            self.pool_config
        )



