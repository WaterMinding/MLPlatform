# 导入标准库模块
import os
import sys
import unittest

# 导入第三方库模块
import duckdb
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
from app import DataCell, Variable
from app import ConstructionError
from app import VariableNotFoundError
from app.protocols import DataConfig

# 测试变量类
class TestVariable(unittest.TestCase):

    # 测试变量类构造方法
    def test_init(self):

        # 输出测试信息
        print("\n测试变量类：构造方法")

        # 测试typechecked装饰器
        with self.assertRaises(TypeCheckError):

            # 测试非字符串类型参数
            Variable(123)
        
        # 构造正常变量字符串
        var_str = "var1:quan:data_12138"

        # 测试传入错误参数
        with self.assertRaises(ConstructionError):

            # 测试错误格式变量字符串
            Variable("var1_quan_data_12138")
        
        # 构造变量对象
        var = Variable(var_str)

        # 断言变量对象属性
        self.assertEqual(var.var_name, "var1")
        self.assertEqual(var.var_type, "quan")
        self.assertEqual(var.owner_id, "data_12138")
        self.assertEqual(var.usage, None)
        self.assertEqual(var.register, None)

        # 保存变量对象
        var_obj = var
        
    # 测试生成变量字符串方法
    def test_to_string(self):

        # 输出测试信息
        print("\n测试变量类：生成变量字符串方法")

        # 构造变量对象
        var_obj = Variable("var1:quan:data_12138")
        
        # 生成变量字符串
        var_str = var_obj.to_string()

        # 断言变量字符串
        self.assertEqual(
            var_str, 
            "var1:quan:data_12138"
        )

    
# 测试数据块类
class TestDataCell(unittest.TestCase):

    # 测试类初始化方法
    def setUp(self):

        # 构造元信息表名
        self.meta_name = "META_TABLE"
        
        # 创建测试用数据库表
        df = DF({"var1": [1, 2, 3]})

        # 将测试用数据表写入数据库
        with duckdb.connect(f"{TESTS}/duck.db") as conn:

            conn.sql(
                "CREATE TABLE data_12138 \
                AS SELECT * FROM df"
            )

            conn.sql(
                f"CREATE TABLE {self.meta_name} " + 
                f"(cell_id VARCHAR, cell_name VARCHAR, " + 
                F"variables TEXT)"
            )

            conn.sql(
                f"INSERT INTO {self.meta_name} VALUES " +
                f"('data_12138', 'data_12138', 'var1:quan:data_12138')"
            )
        
        # 构造数据块对象
        self.data_cell = DataCell(
            
            pool_path = f'{TESTS}/duck.db',
            meta_name = self.meta_name,
            data_config = DataConfig(
                cell_id = "data_12138",
                cell_name = "data_12138",
                var_str_list = [
                    "var1:quan:data_12138"
                ]
            )
        )
    
    # 测试类清理方法
    def tearDown(self):

        # 删除测试用数据库表
        with duckdb.connect(f"{TESTS}/duck.db") as conn:

            conn.sql("DROP TABLE data_12138")
            conn.sql(f"DROP TABLE {self.meta_name}")

        # 删除测试用数据库文件
        os.remove(f"{TESTS}/duck.db")


    # 测试构造方法
    def test_init(self):
        
        # 输出测试信息
        print(
            "\
            \n测试数据块类：构造方法\
            \n测试数据块类：获取数据块ID方法\
            \n测试数据块类：获取变量字符串列表方法\
            \n测试数据块类：获取数据池文件路径方法\
            \n测试数据块类：获取数据块名称方法\
            "
        )

        # 测试typechecked装饰器
        with self.assertRaises(TypeCheckError):    
            
            self.data_cell = DataCell(
                "data_12138",
                self.meta_name,
                "var1:quan:data_12138",
            )
        
        # 构造错误数据配置
        wrong_data_config = DataConfig(
            cell_id = "data_12138",
            cell_name = "data_12138",
            var_str_list = []
        )
        
        # 构造错误数据池文件路径
        wrong_data_pool_file = f'{TESTS}/duckie.db'

        # 测试传入错误数据池文件路径
        with self.assertRaises(FileNotFoundError):

            self.data_cell = DataCell(
                wrong_data_pool_file,
                self.meta_name,
                wrong_data_config
            )

        # 构造正确数据配置
        data_config = DataConfig(
            cell_id = "data_12138",
            cell_name = "data_12138",
            var_str_list = [
                "var1:quan:data_12138"
            ]
        )

        # 正确构造数据块对象
        self.data_cell = DataCell(
            f'{TESTS}/duck.db',
            self.meta_name,
            data_config
        )

        # 测试数据块对象属性
        # 测试数据池文件路径
        self.assertEqual(
            self.data_cell.pool_path,
            f'{TESTS}/duck.db'
        )

        # 测试数据块ID
        self.assertEqual(
            self.data_cell.cell_id,
            "data_12138"
        )

        # 测试数据块名称
        self.assertEqual(
            self.data_cell.cell_name,
            "data_12138"
        )

        # 测试变量字典
        self.assertEqual(
            self.data_cell.var_str_list,
            ["var1:quan:data_12138"]
        )
    
    # 测试查询变量方法
    def test_get_var(self):

        # 输出测试信息
        print("\n测试数据块类：查询变量方法")
        
        # 测试typechecked装饰器
        with self.assertRaises(TypeCheckError):    
            self.data_cell.get_var(123)

        # 测试传入错误变量名
        with self.assertRaises(VariableNotFoundError):
            self.data_cell.get_var("var2")

        # 传入正确变量名
        var = self.data_cell.get_var("var1")

        # 测试变量对象属性
        self.assertEqual(
            var.to_string(),
            "var1:quan:data_12138"
        )

        # 测试变量数据
        self.assertTrue(
            var.register.equals(
                DF({'var1': [1,2,3]})
            )
        )
    
    # 测试追加变量方法
    def test_append_var(self):

        # 输出测试信息
        print("\n测试数据块类：追加变量方法")

        # 测试typechecked装饰器
        with self.assertRaises(TypeCheckError):
            self.data_cell.append_var(123)
        
        # 构造变量对象
        var_2 = Variable("var2:quan:data_12138")
        var_3 = Variable("var3:quan:data_12138")
        var_31 = Variable("var3(1):cate:data_12138")

        # 将数据传入对象
        var_2.register = DF({'var2': [4,5,6]})
        var_3.register = DF({'var3': [7,8,9]})
        var_31.register = DF({'var3(1)': ['a','b','c']})

        # 构造变量列表
        var_lst = [var_2, var_3]

        # 追加非重复变量
        self.data_cell.append_var(var_lst)

        # 检查变量字符串列表
        self.assertEqual(
            self.data_cell.var_str_list,
            [
                "var1:quan:data_12138", 
                "var2:quan:data_12138", 
                "var3:quan:data_12138"
            ]
        )

        # 检查变量数据
        self.assertTrue(
            self.data_cell.get_var(
                "var2"
            ).register.equals(
                DF({'var2': [4,5,6]})
            )
        )

        self.assertTrue(
            self.data_cell.get_var(
                "var3"
            ).register.equals(
                DF({'var3': [7,8,9]})
            )
        )
        
        # 追加重复且无(n)变量
        self.data_cell.append_var([var_3])

        # 检查变量字符串列表
        self.assertEqual(
            self.data_cell.var_str_list,
            [
                "var1:quan:data_12138",
                "var2:quan:data_12138",
                "var3:quan:data_12138",
                "var3(1):quan:data_12138"
            ]
        )

        # 检查变量数据
        self.assertTrue(
            self.data_cell.get_var(
                "var3(1)"
            ).register.equals(
                DF({'var3(1)': [7,8,9]})
            )
        )

        # 追加重复且有(n)变量
        self.data_cell.append_var([var_31])

        # 检查变量字符串列表
        self.assertEqual(
            self.data_cell.var_str_list,
            [
                "var1:quan:data_12138",
                "var2:quan:data_12138",
                "var3:quan:data_12138",
                "var3(1):quan:data_12138",
                "var3(2):cate:data_12138"
            ]
        )

        # 检查变量数据
        self.assertTrue(
            self.data_cell.get_var(
                "var3(2)"
            ).register.equals(
                DF({'var3(2)': ['a','b','c']})
            )
        )

        # 追加多次重复且无(n)变量
        self.data_cell.append_var([var_3])

        # 检查变量字符串列表
        self.assertEqual(
            self.data_cell.var_str_list,
            [
                "var1:quan:data_12138",
                "var2:quan:data_12138",
                "var3:quan:data_12138",
                "var3(1):quan:data_12138",
                "var3(2):cate:data_12138",
                "var3(3):quan:data_12138",
            ]
        )

        # 检查变量数据
        self.assertTrue(
            self.data_cell.get_var(
                "var3(3)"
            ).register.equals(
                DF({'var3(3)': [7,8,9]})
            )
        )

        # 追加多次重复且有(n)变量
        self.data_cell.append_var([var_31])

        # 检查变量字符串列表
        self.assertEqual(
            self.data_cell.var_str_list,
            [
                "var1:quan:data_12138",
                "var2:quan:data_12138",
                "var3:quan:data_12138",
                "var3(1):quan:data_12138",
                "var3(2):cate:data_12138",
                "var3(3):quan:data_12138",
                "var3(4):cate:data_12138",
            ]
        )

        # 检查变量数据
        self.assertTrue(
            self.data_cell.get_var(
                "var3(4)"
            ).register.equals(
                DF({'var3(4)': ['a','b','c']})
            )
        )

        # 读取数据池文件元信息
        with duckdb.connect(f"{TESTS}/duck.db") as conn:
            
            meta = conn.sql(
                f"SELECT * FROM {self.meta_name} " +
                f"WHERE cell_id = '{self.data_cell.cell_id}'"
            ).df().values[0].tolist()
        
        # 检查元信息
        self.assertEqual(
            meta,
            [
                self.data_cell.cell_id,
                self.data_cell.cell_id,
                ';'.join(self.data_cell.var_str_list),
            ]
        )
    
    # 测试替换变量方法
    def test_replace_var(self):

        # 输出测试信息
        print("\n测试数据块类：替换变量方法")

        # 构造变量对象
        var_1 = Variable("var1:quan:data_12138")
        var_2 = Variable("var2:cate:data_12138")

        # 将数据添加到变量对象
        var_1.register = DF({'var1': [1,3,9]})
        var_2.register = DF({'var2': ['a','b','c']})

        # 检查typechecked装饰器
        with self.assertRaises(TypeCheckError):
            self.data_cell.replace_var([456456])

        # 检查传入错误变量
        with self.assertRaises(VariableNotFoundError):
            self.data_cell.replace_var([var_2])

        # 检查传入正确变量
        self.data_cell.replace_var([var_1])

        # 检查变量数据
        self.assertTrue(
            self.data_cell.get_var(
                "var1"
            ).register.equals(
                DF({'var1': [1,3,9]})
            )
        )

    # 测试获取数据配置方法
    def test_get_config(self):

        # 输出测试信息
        print("\n测试数据块类：获取数据配置方法")
        
        # 检查数据配置
        self.assertEqual(
            self.data_cell.get_config(),
            DataConfig(
                cell_id = "data_12138",
                cell_name = "data_12138",
                var_str_list = [
                    "var1:quan:data_12138",
                ]                
            )
        )