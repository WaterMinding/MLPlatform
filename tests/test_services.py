# 导入标准库模块
import os
import base64
import shutil
import asyncio
import unittest
import importlib
import tomllib as toml
from io import BytesIO

# 导入第三方库模块
import pandas as pd
from PIL import Image
from duckdb import connect
from fastapi import UploadFile
from pandas import DataFrame as DF
from duckdb import CatalogException
from matplotlib import pyplot as plt

# 导入自定义模块
from app import CellType
from app import DataPool
from app import DataConfig
from app import TextConfig
from app import DocuConfig
from app import OpConfigFront
from app import run_op
from app import fifolock
from app import open_docu
from app import initialize
from app import ImageConfig
from app import upload_data
from app import delete_data
from app import import_data
from app import get_pool_meta
from app import DataNotFoundError
from app import DocuNotFoundError
from app import VarUsageNotFoundError
from app import VariableNotFoundError

FILE_PATH = os.path.abspath(__file__)
TESTS = os.path.dirname(FILE_PATH)
ROOT = os.path.dirname(TESTS)
print(ROOT)


# 服务层测试类
class TestServices(unittest.TestCase):

    # 测试类初始化方法
    def setUp(self):

        # 移动配置文件
        shutil.move(
            src = f"{ROOT}/app/config.toml",
            dst = f"{TESTS}/config.toml."
        )
        
        self.test_data_path = f"{TESTS}/test_data/".replace("\\", "/")
        self.pool_path = f"{TESTS}/test_data/duck.db".replace("\\", "/")
        self.cache_path = f"{TESTS}/test_data/cache/".replace("\\", "/")
        self.pool_meta = "META_TABLE"

        # 构造新的配置文件
        with open(
            file = f"{ROOT}/app/config.toml",
            encoding = "utf-8",
            mode = "w"
        ) as file:
            
            config = f'data_pool_path = "{self.pool_path}"\n' + \
                    f'data_cache_path = "{self.cache_path}"'

            file.write(config)

        # 构造测试用数据池文件
        os.makedirs(self.test_data_path, exist_ok = True)
        os.makedirs(self.cache_path, exist_ok = True)

        with connect(
            database = self.pool_path,
        ) as conn:
            
            conn.sql(
                f"CREATE TABLE IF NOT EXISTS {self.pool_meta}" + 
                f" (cell_id VARCHAR PRIMARY KEY, " 
                f"cell_name VARCHAR, variables VARCHAR)"
            )
        
        # 构造测试用数据
        self.test_id = "data_12138"

        df = DF(
            data = {
                "X": [1, 2, 3],
                "Y": ['a', 'b', 'c'],
            }
        )

        with connect(
            database = self.pool_path,
        ) as conn:

            conn.register(
                python_object = df,
                view_name = self.test_id
            )
            
            conn.sql(
                f"CREATE TABLE {self.test_id} AS " +
                f"SELECT * FROM {self.test_id}"
            )

            conn.sql(
                f"INSERT INTO {self.pool_meta}" +
                f"(cell_id, cell_name, variables) VALUES " +
                f"('{self.test_id}', '{self.test_id}', "
                f"'X:quan:data_12138;Y:cate:data_12138')"
            )
    
        # 构造测试用文件
        self.test_csv_path = f"{self.test_data_path}/test_data.csv"
        self.test_excel_path = f"{self.test_data_path}/test_data.xlsx"
            
        df.to_csv(
            self.test_csv_path, 
            index = False
        )

        df.to_excel(
            self.test_excel_path,
            index = False
        )

        # 构造测试用算子
        OP_PATH = f'{ROOT}/app/operators/'
        
        with open(
            f'{OP_PATH}/test_op2.py', 'w',
            encoding = 'utf-8'
        ) as script:
            
            # 写入测试用算子脚本
            script.write(
                "from ..protocols import OpResult\n" +
                "from ..protocols import TextConfig\n" +
                "from ..protocols import ChartConfig\n" +
                "from ..data import Variable\n" +
                "from copy import deepcopy\n" +
                "from ..protocols import VarUsage\n" +
                "from pandas import DataFrame as DF\n" +
                "class TestOp2:\n" +
                "    def __init__(self,params,variables):\n" +
                "        self.var1 = variables['var1'][0]\n" +
                "        self.var2 = variables['var2'][0]\n" +
                "    def run(self, *args, **kwargs):\n" +
                "        self.var1.register = DF({'X':[2,4,6]})\n" +
                "        self.var1.usage = VarUsage.REPLACE\n" +
                "        self.var2.usage = VarUsage.APPEND\n" +
                "        return OpResult(\n" +
                "            text_list = None,\n" +
                "            chart_list = None,\n" +
                "            data_list = [self.var1,self.var2]\n" +
                "        )\n" +
                "    def check(self, *args, **kwargs):\n" +
                "        pass\n"
            )
        
        # 在算子层配置文件中注册测试用算子
        with open(
            f'{OP_PATH}/op_config.toml', 'a',
            encoding = 'utf-8'
        ) as config:

            # 写入测试用算子配置
            config.write(
                'TestOp2 = "test_op2.TestOp2"'
            )

    # 测试类清理方法
    def tearDown(self):

        # 删除测试用数据文件夹
        shutil.rmtree(
            self.test_data_path
        )

        # 删除测试用配置文件
        os.remove(
            f"{ROOT}/app/config.toml"
        )

        # 恢复原始配置文件
        shutil.move(
            src = f"{TESTS}/config.toml",
            dst = f"{ROOT}/app/config.toml"
        )

        # 确定算子层路径
        OP_PATH = f'{ROOT}/app/operators/'

        # 删除测试用算子脚本
        os.remove(f'{OP_PATH}/test_op2.py')

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
                if 'TestOp2' not in line:
                    config.write(line)

    # 测试初始化服务
    def test_initial(self):

        # 输出测试信息
        print('\n测试服务层：初始化服务')

        # 删除测试类构造的数据池文件和缓存文件夹
        shutil.rmtree(
            self.test_data_path
        )

        # 执行初始化函数
        pool_path = asyncio.run(
            initialize(self.pool_meta)
        )

        # 验证返回值
        self.assertEqual(
            pool_path,
            self.pool_path
        )

        # 验证数据池文件是否存在
        self.assertTrue(
            os.path.exists(
                self.pool_path
            )
        )

        # 验证缓存文件夹是否存在
        self.assertTrue(
            os.path.exists(
                self.cache_path
            )
        )

        # 检查数据池文件元信息表
        with connect(self.pool_path) as conn:

            meta = conn.sql(
                f"SELECT * FROM {self.pool_meta}"
            ).df()
        
        self.assertEqual(
            list(meta.columns),
            
            [
                'cell_id', 
                'cell_name', 
                'variables'
            ]
        )

    # 测试数据上传服务
    def test_upload_data(self):

        # 输出测试信息
        print('\n测试服务层：数据上传服务')

        # 构造测试文件
        csv = open(
            self.test_csv_path,
            mode = 'r',
            encoding='utf-8'
        )

        excel = open(
            self.test_excel_path,
            mode = 'rb',
        )
            
        up_csv = UploadFile(
            filename = 'test.csv',
            file = csv
        )

        up_excel = UploadFile(
            filename = 'test.xlsx',
            file = excel
        )

        # 调用数据上传服务
        csv_result = asyncio.run(
            upload_data(
                up_csv,
                self.pool_path,
                self.pool_meta
            )
        )

        excel_result = asyncio.run(
            upload_data(
                up_excel,
                self.pool_path,
                self.pool_meta
            )
        )

        # 验证上传结果的数据块ID
        self.assertTrue(
            "data_" in csv_result[0]
        )

        self.assertTrue(
            "data_" in excel_result[0]
        )

        # 验证上传结果的数据块名称
        self.assertEqual(
            csv_result[1],
            'test.csv'
        )

        self.assertEqual(
            excel_result[1],
            'test.xlsx'
        )

        # 验证上传结果的数据块变量
        self.assertEqual(
            csv_result[2],
            
            f'X:quan:{csv_result[0]};' +
            f'Y:cate:{csv_result[0]}'
        )

        self.assertEqual(
            excel_result[2],

            f'X:quan:{excel_result[0]};' +
            f'Y:cate:{excel_result[0]}'
        )

        # 验证数据池文件中是否有对应数据块
        with connect(self.pool_path,)as conn:

            csv_data = conn.sql(
                f"select * from {csv_result[0]}"
            ).df()

            excel_data = conn.sql(
                f"select * from {excel_result[0]}"
            ).df()

            # 验证数据块中是否有数据
            self.assertTrue(
                not csv_data.empty
            )

            self.assertTrue(
                not excel_data.empty
            )

            # 验证数据块中是否有对应变量
            self.assertEqual(
                csv_data.columns.tolist(),
                ['X', 'Y']
            )

            self.assertEqual(
                excel_data.columns.tolist(),
                ['X', 'Y']
            )

        # 关闭测试文件
        csv.close()
        excel.close()

        # 检查传入不支持格式文件
        with open(
            f"{self.test_data_path}/test.kkk",
            mode = 'w'
        ) as kkk:
            
            with self.assertRaises(ValueError):

                asyncio.run(
                    upload_data(
                        UploadFile(
                            file = kkk,
                            filename = 'test.kkk'
                        ),
                        self.pool_path,
                        self.pool_meta
                    )
                )

        # 测试处理重复列名能力
        with open(
            f"{self.test_data_path}/test.csv",
            encoding = 'utf-8',
            mode = 'w',
        ) as csv:
            
            csv.write(
                'X,X,Y(1),Y(1)\n' +
                '1,2,3,4\n'
            )

        csv = open(
            f"{self.test_data_path}/test.csv",
            encoding = 'utf-8',
            mode = 'r'
        )

        asyncio.run(
            upload_data(
                UploadFile(
                    file = csv,
                    filename = 'test.csv'
                ),
                self.pool_path,
                self.pool_meta
            )
        )

        csv.close()

    # 测试数据删除服务
    def test_delete_data(self):

        # 输出测试信息
        print('\n测试服务层：数据删除服务')

        # 调用数据删除服务，删除不存在的数据
        with self.assertRaises(DataNotFoundError):

            asyncio.run(
                delete_data(
                    pool_path = self.pool_path,
                    pool_meta = self.pool_meta,
                    cell_id = 'not_exist_data'
                )
            )
        
        # 调用数据删除服务，删除存在的数据
        deleted_data = asyncio.run(
            delete_data(
                pool_path = self.pool_path,
                pool_meta = self.pool_meta,
                cell_id = self.test_id
            )
        )

        # 验证删除的数据是否正确
        deleted_id = deleted_data[0]
        deleted_name = deleted_data[1]
        deleted_vars = deleted_data[2]

        self.assertEqual(
            deleted_id,
            self.test_id
        )

        self.assertEqual(
            deleted_name,
            self.test_id
        )

        self.assertEqual(
            deleted_vars,
            f"X:quan:{self.test_id};" +
            f"Y:cate:{self.test_id}"
        )

        # 验证元数据是否被删除
        with connect(self.pool_path) as conn:

            meta = conn.sql(
                f"SELECT * FROM {self.pool_meta} " + 
                f"WHERE cell_id = '{self.test_id}'"
            ).df()
        
        self.assertTrue(meta.empty)

        # 验证数据是否被删除
        with self.assertRaises(CatalogException):
            with connect(self.pool_path) as conn:

                data = conn.sql(
                    f"SELECT * FROM {self.test_id}"
                ).df()

    # 测试获取数据池文件元信息服务
    def test_get_pool_meta(self):

        # 输出测试信息
        print("\n测试服务层：获取数据池文件元信息服务")

        # 获取数据池文件元数据信息
        with connect(self.pool_path) as conn:

            meta = conn.sql(
                f"SELECT * FROM {self.pool_meta} "
            ).df().to_dict(orient="index")

        test_meta = asyncio.run(
            get_pool_meta(
                self.pool_path,
                self.pool_meta
            )
        )

        self.assertEqual(
            meta,
            test_meta
        )

    # 测试导入数据服务
    def test_import_data(self):

        # 输出测试信息
        print("\n测试服务层：导入数据服务")

        data_pool = DataPool(
            self.pool_path,
            self.pool_meta
        )
        
        # 测试DocuNotFoundError
        with self.assertRaises(DocuNotFoundError):

            asyncio.run(
                import_data(
                    self.test_id,
                    None
                )
            )
        
        # 测试DataNotFoundError
        with self.assertRaises(DataNotFoundError):

            asyncio.run(
                import_data(
                    '12345',
                    data_pool
                )
            )

        # 测试导入数据
        asyncio.run(
            import_data(
                self.test_id,
                data_pool
            )
        )

        data_config = data_pool.get_cell(
            self.test_id
        ).get_config()

        self.assertEqual(
            data_config.cell_id,
            self.test_id
        )

        self.assertEqual(
            data_config.cell_name,
            self.test_id
        )

        self.assertEqual(
            data_config.var_str_list,
            [
                f"X:quan:{self.test_id}",
                f"Y:cate:{self.test_id}"
            ]
        )

    # 测试打开文档服务
    def test_open_doc(self):

        # 输出测试信息
        print('\n测试服务层：打开文档服务')

        # 构造文档配置
        img_config = ImageConfig(
            cell_type = CellType.IMAGE,
            cell_num = 0,
            image = 'abcdefg'
        )

        text_config = TextConfig(
            cell_type = CellType.TEXT,
            cell_num = 0,
            text = 'abcdefg'
        )

        data_config = DataConfig(
            cell_id = self.test_id,
            cell_name = self.test_id,
            var_str_list = [
                f"X:quan:{self.test_id}",
                f"Y:cate:{self.test_id}"
            ]
        )

        data_config_2 = DataConfig(
            cell_id = 'asdsa',
            cell_name = 'dsadsa',
            var_str_list = []
        )

        docu_config = DocuConfig(
            docu_name = 'asd',
            edit_list = [
                img_config,
                text_config
            ],
            data_list = [
                data_config,
                data_config_2
            ]
        )

        # 打开文档
        data_pool, missing_data = asyncio.run(
            open_docu(
                docu_config = docu_config,
                pool_path = self.pool_path,
                pool_meta = self.pool_meta
            )
        )

        # 检查data_pool
        pool_config = data_pool.get_config()
        
        self.assertEqual(
            list(pool_config.values()),
            [data_config]
        )

        # 检查missing_data
        self.assertEqual(
            missing_data,
            {'asdsa': 'dsadsa'}
        )

    # 测试算子执行服务
    def test_run_op(self):

        # 输出测试信息
        print('\n测试服务层：算子执行服务')

        # 构造正确的线性回归算子配置
        op_config_lr = OpConfigFront(
            cell_type = CellType.OP,
            op_name = 'LinearRegression',
            parameters = None,
            variables = {
                'X': [f"X:quan:{self.test_id}"],
                'Y': [f"X:quan:{self.test_id}"]
            }
        )

        # 构造数据池
        data_pool = DataPool(
            pool_path = self.pool_path,
            meta_name = self.pool_meta
        )

        data_pool.add_cell(
            data_config = DataConfig(
                cell_id = self.test_id,
                cell_name = self.test_id,
                var_str_list = [
                    f"X:quan:{self.test_id}",
                    f"Y:cate:{self.test_id}"
                ]
            )
        )

        # 执行线性回归算子
        op_result = asyncio.run(
            run_op(
                op_config = op_config_lr,
                data_pool = data_pool
            )
        )

        # 检查结果文本区
        self.assertEqual(
            op_result.text_list[0].text,
            '$$y = 0.9999999999999998x + 4.440892098500626e-16$$'
        )

        # 输出结果图像
        image_data = base64.b64decode(
            op_result.chart_list[0].image
        )

        with Image.open(BytesIO(image_data)) as img:
            plt.imshow(img)
            plt.axis('off')
            plt.show()
            plt.close()

        # 构造正确的测试算子配置
        op_config_tt = OpConfigFront(
            cell_type = CellType.OP,
            op_name = 'TestOp2',
            parameters = None,
            variables = {
                'var1': [f"X:quan:{self.test_id}"],
                'var2': [f"Y:cate:{self.test_id}"]
            }
        )

        # 执行算子
        op_result = asyncio.run(
            run_op(
                op_config = op_config_tt,
                data_pool = data_pool
            )
        )

        # 检查结果
        self.assertEqual(
            op_result.text_list,
            None
        )

        self.assertEqual(
            op_result.chart_list,
            None
        )

        # 检查数据块更改情况
        cell = data_pool.get_cell(
            self.test_id
        )

        self.assertEqual(
            cell.get_config().var_str_list,
            [
                f"X:quan:{self.test_id}",
                f"Y:cate:{self.test_id}",
                f"Y(1):cate:{self.test_id}",
            ]
        )
        
        self.assertTrue(
            DF({'X': [2, 4, 6],}).equals(
                cell.get_var('X').register
            )
        )

        # 传入含有错误变量的配置
        op_config_er = OpConfigFront(
            op_name = 'TestOp2',
            cell_type = CellType.OP,
            parameters = None,
            variables = {
                'var1': [f"X:quan:{self.test_id}"],
                'var2': [f"Z:cate:{self.test_id}"]
            }
        )

        with self.assertRaises(VariableNotFoundError):
            
            asyncio.run(
                run_op(
                    op_config = op_config_er,
                    data_pool = data_pool
                )
            )
        
        # 测试运行时数据池不存在的情况
        with self.assertRaises(DocuNotFoundError):

            asyncio.run(
                run_op(
                    op_config = op_config_lr,
                    data_pool = None
                )
            )

        # 测试算子执行错误
        with connect(self.pool_path) as conn:

            conn.sql(
                f"UPDATE {self.test_id}" +
                f" SET X = NULL"
            )
        
        with self.assertRaises(ValueError):

            asyncio.run(
                run_op(
                    op_config = op_config_lr,
                    data_pool = data_pool
                )
            )
        
        # 测试VarUsageNotFoundError
        OP_PATH = f'{ROOT}/app/operators/'

        with open(
            f'{OP_PATH}/test_op2.py', 'r',
            encoding = 'utf-8'
        ) as script:
        
            code = script.read()
            code = code.replace(
                'VarUsage.REPLACE', 
                "'ABC'"
            )

        with open(
            f'{OP_PATH}/test_op2.py', 'w',
            encoding = 'utf-8'
        ) as script:

            script.write(code)

        modu = "test_op2"
        modu = importlib.import_module(
            'app.operators.' + modu,
            package = 'app.operators'
        )
        importlib.reload(modu)

        with self.assertRaises(VarUsageNotFoundError):

            asyncio.run(
                run_op(
                    op_config = op_config_tt,
                    data_pool = data_pool
                )
            )

    # 测试FIFOlock
    def test_fifo_lock(self):

        # 输出测试信息
        print('\n测试异步公平锁')

        lock = fifolock.FIFOLock()

        # 构造从协程函数
        async def worker(lock,name:str):
           print(f"{name} 尝试获取锁")
           async with lock:
                print(f"{name} 已经获取锁")
                print(f"{name} 开始执行")
                await asyncio.sleep(2)
                print(f"{name} 执行结束")
                return name
        
        # 构造主协程函数
        async def co_main(lock):

            names = await asyncio.gather(
                worker(lock,'A'),
                worker(lock,'B'),
                worker(lock,'C'),
                worker(lock,'D'),
            )

            return names
        
        # 启动主协程函数
        names = asyncio.run(co_main(lock))

        # 检查输出结果
        self.assertEqual(names, ['A', 'B', 'C', 'D'])
                
