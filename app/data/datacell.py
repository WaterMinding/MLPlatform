# 导入标准库模块
import re
import os
from copy import deepcopy

# 导入第三方库模块
import duckdb
from pydantic import BaseModel
from typeguard import typechecked
from pandas import DataFrame as DF
from duckdb import DuckDBPyConnection as DBC

# 导入自定义模块
from ..mlp_exceptions import VariableNotFoundError
from ..mlp_exceptions import ConstructionError


# 定义数据配置协议
# 为避免与protocols包的循环依赖
# 这里将协议定义在data包中
class DataConfig(BaseModel):

    # 数据块ID
    cell_id: str

    # 数据表名
    cell_name: str

    # 变量列表
    var_str_list: list[str]


# 变量类
class Variable:

    # 构造方法
    # 参数1：var_str - 变量字符串
    @typechecked
    def __init__(
        self,
        var_str: str
    ):
        
        # 解析变量字符串
        try:
        
            split_results = var_str.split(':')
            self.var_name = split_results[0]
            self.var_type = split_results[1]
            self.owner_id = split_results[2]

        except Exception:

            raise ConstructionError('Variable object')

        # 初始化使用方法
        self.usage: str|None = None

        # 初始化寄存DF对象
        self.register: DF|None = None
    
    # 生成变量字符串方法
    def to_string(self) -> str:

        var_name = f'{self.var_name}'
        var_type = f':{self.var_type}'
        owner_id = f':{self.owner_id}'

        return var_name + var_type + owner_id


# 数据块类
class DataCell:

    # 构造方法
    # 参数1：pool_path - 数据池文件路径
    # 参数2：data_config - 数据配置
    @typechecked
    def __init__(
        self,
        pool_path: str,
        meta_name: str,
        data_config: DataConfig
    ):
        
        try:

            # 保存数据池文件路径
            self.__pool_path = pool_path

            # 保存元信息表名
            self.__meta_name = meta_name

            # 检查数据池文件是否存在
            if not os.path.exists(pool_path):

                raise FileNotFoundError(pool_path)

            # 读取数据块ID
            self.__cell_id: str = data_config.cell_id

            # 生成变量对象列表
            var_list = map(
                Variable,
                data_config.var_str_list
            )

            # 读取数据名
            self.__cell_name: str = data_config.cell_name

            # 生成变量字典
            self.__var_dict: dict[str, Variable] = {
                var.var_name: var for var in var_list
            }
        
        except FileNotFoundError as fne:

            raise fne

    # 获取数据块ID方法
    @property
    def cell_id(self) -> str:

        return self.__cell_id
    
    # 获取变量字符串列表方法
    @property
    def var_str_list(self) -> list[str]:

        # 获取变量字符串列表
        var_str_list = [
            var.to_string() 
            for var in self.__var_dict.values()
        ]

        return var_str_list
    
    # 获取数据池文件路径方法
    @property
    def pool_path(self) -> str:

        return self.__pool_path
    
    # 获取数据块名称方法
    @property
    def cell_name(self) -> str:

        return self.__cell_name

    # 变量查询方法
    # 参数1：var_name - 变量名
    @typechecked
    def get_var(self, var_name: str) -> Variable:
        
        # 如果变量名不存在
        if var_name not in self.__var_dict:

            # 抛出异常
            raise VariableNotFoundError(
                var_name
            )
        
        # 读取列数据
        with duckdb.connect(self.__pool_path) as conn:
            
            var_data = conn.execute(
                f'SELECT "{var_name}" FROM {self.__cell_id};'
            ).df()

        # 获取变量对象
        var = self.__var_dict[var_name]

        # 构造变量对象
        new_var = Variable(
            var.to_string()
        )

        # 设置变量数据
        new_var.register = var_data

        # 返回变量对象
        return new_var
    
    # 追加变量方法
    # 参数1：var_list - 变量对象列表
    @typechecked
    def append_var(
        self, 
        var_lst: list[Variable]
    ):
        
        # 读取原始表
        with duckdb.connect(self.__pool_path) as conn:

            origin_data = conn.sql(
                f'SELECT * FROM {self.cell_id}'
            ).df()

        # 复制变量列表
        var_list = deepcopy(var_lst)

        # 遍历变量对象列表
        for var in var_list:

            # 规范变量owner_id
            var.owner_id = self.__cell_id
            
            # 如果变量名重复，调整新变量名
            while var.var_name in self.__var_dict or \
                var.var_name == 'CONNECT_ROW_ID':

                # 判断新变量名末尾是否包含(n)，其中n为正整数
                pattern  = r'\([1-9]\d*\)$'
                match_obj = re.search(
                    pattern, 
                    var.var_name
                )

                # 如果新变量名末尾不包含(n)
                if not match_obj:

                    new_var_name = var.var_name + '(1)'
                    
                    # 则在其末尾添加(1)
                    var.register.rename(
                        
                        columns={
                            var.var_name: new_var_name
                        },

                        inplace = True
                    )

                    # 更新变量名
                    var.var_name = new_var_name
                
                # 如果新变量名末尾包含(n)
                else:

                    # 获取 n
                    n = int(match_obj.group()[1:-1])

                    # 使用(n+1)替换(n)
                    new_var_name = re.sub(
                        pattern,
                        f'({n+1})',
                        var.var_name
                    )

                    # 更新数据列名
                    var.register.rename(
                        
                        columns={
                            var.var_name: new_var_name
                        },

                        inplace = True
                    )

                    # 更新变量名
                    var.var_name = new_var_name
        
            # 原始表新增列
            origin_data[var.var_name] = var.register

            # 保存变量对象
            var.register = None
            var.usage = None
            self.__var_dict[var.var_name] = var
        
        # 保存数据到数据库
        with duckdb.connect(self.__pool_path) as conn:

            conn.sql(
                f'CREATE OR REPLACE TABLE \
                "{self.cell_id}" AS \
                SELECT * FROM origin_data'
            )

        # 构造长变量字符串
        long_var_str = ';'.join(self.var_str_list)

        # 保存长变量字符串
        with duckdb.connect(self.__pool_path) as conn:

            conn.sql(
                f"UPDATE '{self.__meta_name}' " +
                f"SET variables = '{long_var_str}' " +
                f"WHERE cell_id = '{self.cell_id}'"
            )

    # 替换变量方法
    # 参数1：var_list - 变量对象列表
    @typechecked
    def replace_var(self, var_lst: list[Variable]):

        # 读取原始表
        with duckdb.connect(self.__pool_path) as conn:

            origin_data = conn.execute(
                f'SELECT * FROM {self.__cell_id}'
            ).df()

        # 复制变量列表
        var_list = deepcopy(var_lst)

        # 遍历变量对象列表
        for var in var_list:

            # 规范化变量owner_id
            var.owner_id = self.__cell_id
            
            # 如果变量名不存在
            if var.var_name not in self.__var_dict:

                # 抛出异常
                raise VariableNotFoundError(
                    var.var_name
                )

            # 替换数据
            origin_data[var.var_name] = var.register

        # 保存数据到数据库
        with duckdb.connect(self.__pool_path) as conn:

            conn.sql(
                f'CREATE OR REPLACE TABLE \
                {self.cell_id} AS \
                SELECT * FROM origin_data'
            )
    
    # 获取数据配置方法
    def get_config(self):

        # 生成数据配置
        config = {
            'cell_id': self.__cell_id,
            'cell_name': self.__cell_name,
            'var_str_list': [
                self.__var_dict[var_name].to_string()
                for var_name in self.__var_dict
            ]
        }

        config = DataConfig(**config)

        # 返回数据配置
        return config