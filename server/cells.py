# 导包
import re
import h5py
import asyncio
import pandas as pd
import mlp_exception
from copy import deepcopy
from typeguard import typechecked
from pandas import DataFrame as DF
from pandas import Series as SR


# 文本块
class TextCell:

    # 构造方法
    @typechecked
    def __init__(self, text_config:dict):
        
        # 根据传入的文本配置字典，初始化文本块
        try:
            # 文本块ID
            self.__cell_id:str = text_config['cell_id']

            # 文本字符串
            self.__text:str = text_config['text']

        # 处理可能存在的异常
        except Exception as e:

            # 抛出文本块构造异常
            raise mlp_exception.ConstructionError(
                "TextCell"
            )
    
    # 获取文本块ID方法
    def get_cell_id(self):

        # 返回文本块ID
        return self.__cell_id

    # 生成文本配置字典方法
    def get_config(self):

        # 将属性值存入字典
        text_config = {
            'cell_id': self.__cell_id,
            'text': self.__text
        }

        # 返回文本配置字典
        return text_config
    
    # 返回文本字符串方法
    def get_text(self):

        # 返回文本字符串
        return self.__text
    
    # 编辑文本字符串方法
    @typechecked
    def edit_text(self, new_text:str):

        # 使用新字符串覆盖文本字符串
        self.__text = new_text


# 算子块
class OpCell:

    # 构造方法
    @typechecked
    def __init__(self, op_config:dict):

        # 根据传入的算子配置字典，初始化算子块
        try:
            # 算子块ID
            self.__cell_id:str = op_config['cell_id']

            # 算子类型
            self.__op_type:str = op_config['op_type']

            # 参数字典
            self.__params:dict = op_config['params']

            # 变量字典
            self.__vars:dict = op_config['vars']

        # 处理可能存在的异常
        except Exception as e:

            # 抛出算子块构造异常
            raise mlp_exception.ConstructionError(
                "OpCell"
            )
        
    # 获取算子块ID
    def get_cell_id(self):

        # 返回算子块ID
        return self.__cell_id
    
    # 生成算子配置字典方法
    def get_config(self):

        # 属性值存入字典
        op_config = {
            'cell_id':self.__cell_id,
            'op_type':self.__op_type,
            'params':self.__params,
            'vars':self.__vars
        }

        # 返回算子配置字典
        return op_config

    # 生成属性克隆列表方法
    def get_attributes_clone(self):

        # 定义属性克隆列表
        clones = [
            self.__op_type,
            deepcopy(self.__params),
            deepcopy(self.__vars)
        ]

        # 返回属性克隆列表
        return clones
    
    # 编辑参数/变量字典方法
    @typechecked
    def edit_content(self, to_edit:str, new_content:dict):

        # 如果编辑项是参数字典
        if to_edit == 'params':

            # 覆盖参数字典
            self.__params = new_content

        # 如果编辑项是变量字典
        elif to_edit == 'vars':

            # 覆盖变量字典
            self.__vars = new_content

        else:
            
            # 抛出值异常
            raise ValueError('to_edit must be "params" or "vars"')


# 元素类
class Element:

    # 构造方法
    @typechecked
    def __init__(self,elem_type:str, docu_inner_path:str):
        
        # 根据传入的参数，初始化元素
        try:
            # 保存元素类型
            self.__elem_type = elem_type

            # 保存文档文件内路径
            self.__docu_inner_path = docu_inner_path
        
        # 处理可能出现的异常
        except Exception as e:

            # 抛出元素构造异常
            raise mlp_exception.ConstructionError(
                "Element"
            )
    
    # 生成元素配置字典方法
    @typechecked
    async def get_elem_config(self, with_data:bool, docu_path:str):

        # 如果with_data为True
        if with_data:

            # 读取元素数据
            data = await asyncio.to_thread(self.__get_data(docu_path))

            # 将数据和元素类型打包成字典
            elem_config = {
                'elem_type': self.__elem_type,
                'elem_data': data
            }

            # 返回元素配置字典
            return elem_config

        # 如果with_data为False
        else:

            # 将文档文件内路径和元素类型打包为元素配置字典
            elem_config = {
                'elem_type': self.__elem_type,
                'elem_data': self.__docu_inner_path
            }

            # 返回元素配置字典
            return elem_config
    
    # 读取元素数据方法
    @typechecked
    def __get_data(self, docu_path:str):

        # 打开文档文件
        with pd.HDFStore(docu_path) as store:

            # 读取元素数据
            data = store[self.__docu_inner_path]
        
        # 返回元素数据
        return data
        

# 图像块
class ChartCell:

    # 构造方法
    @typechecked
    def __init__(self, chart_config:dict, docu_path:str):

        # 根据传入的图像配置字典，初始化图像块
        try:
            # 保存块ID
            self.__cell_id:str = chart_config['cell_id']
            
            # 保存文档文件路径
            self.__docu_path:str = docu_path

            # 初始化元素列表
            self.__elem_list:list = []

            # 获取字典内元素配置列表
            elem_config_list = chart_config['elem_config_list']

            # 如果元素数据类型为DataFrame
            if chart_config['elem_data_type'] == 'DF':

                # 在文档文件中创建该图像块的文件内目录
                with h5py.File(self.__docu_path, 'a') as file:

                    # 在文件内的图像数据组目录中创建组
                    file['Chart'].create_group(self.__cell_id)

                    # 初始化计数器
                    counter = 0

                    # 逐个生成并保存元素对象及其数据
                    for elem_config in elem_config_list:

                        # 保存元素数据
                        elem_data = elem_config['elem_data']
                        file['Chart'][self.__cell_id].create_dataset(
                            name = str(counter),
                            data = elem_data
                        )

                        # 保存元素对象
                        self.__elem_list.append(
                            Element(
                                elem_type = elem_config['elem_type'],
                                docu_inner_path = f'Chart/{self.__cell_id}/{counter}'
                            )
                        )

                        # 更新计数器
                        counter += 1

            # 如果元素数据类型为str
            elif chart_config['elem_data_type'] == 'str':

                # 初始化计数器
                counter = 0

                # 逐个生成并保存元素对象及其数据于文件内的路径
                for elem_config in elem_config_list:

                    # 保存元素对象
                    self.__elem_list.append(
                        Element(
                            elem_type = elem_config['elem_type'],
                            docu_inner_path = f'Chart/{self.__cell_id}/{counter}'
                        )
                    )

                    # 更新计数器
                    counter += 1

            # 如果元素数据类型为其他
            else:

                # 抛出异常
                raise ValueError()

        # 处理可能存在的异常
        except Exception as e:

            # 抛出异常
            raise mlp_exception.ConstructionError(
                    'ChartCell'
            )

    # 获取图像块ID方法
    def get_cell_id(self):

        # 返回图像块ID
        return self.__cell_id
    
    # 生成图像配置字典方法
    @typechecked
    async def get_config(self, with_data:bool):

        # 构建协程列表
        coro_list = [
            elem.get_config(with_data) for elem in self.__elem_list
        ]

        # 并发执行协程列表,获取元素列表
        elem_config_list = await asyncio.gather(*coro_list)

        # 定义元素数据类型
        if with_data:
            elem_data_type = 'DF'
        else:
            elem_data_type = 'str'

        # 打包并返回图像配置字典
        return {
            'cell_id': self.__cell_id,
            'elem_data_type': elem_data_type,
            'elem_config_list': elem_config_list
        }


# 变量类
class Variable:

    # 构造方法
    @typechecked
    def __init__(self, var_str:str):

        # 根据传入的变量字符串，初始化变量对象
        try:
            # 解析变量字符串
            var_name,var_type,owner_id = var_str.split(':')

            # 保存变量名
            self.var_name:str = var_name

            # 保存变量类型
            self.var_type:str = var_type

            # 保存所属块ID
            self.owner_id:str = owner_id

            # 定义变量使用方法
            self.usage:str = ''

            # 定义寄存DF对象
            self.df_register:DF = None
        
        # 处理可能出现的异常
        except Exception as e:
            
            raise mlp_exception.ConstructionError('Variable')
    
    # 生成变量字符串方法
    def get_var_str(self):

        # 返回变量字符串
        return f'{self.var_name}:{self.var_type}:{self.owner_id}'


# 数据块
class DataCell:

    # 构造方法
    @typechecked
    def __init__(self, docu_path:str, data_config:dict):

        # 根据传入的数据配置字典及参数，初始化数据块对象
        try:
            # 定义寄存DF对象
            self.__df_register:DF|SR|None = None

            # 保存文档文件路径
            self.__docu_path:str = docu_path

            # 保存数据块ID
            self.__cell_id:str = data_config['cell_id']

            # 读取变量字符串列表
            var_str_list:list[str] = data_config['var_str_list']

            # 生成变量字典
            self.__var_dict = {}
            for var_str in var_str_list:

                # 生成变量对象
                var = Variable(var_str)

                # 保存变量对象
                self.__var_dict[var.var_name] = var
        
        # 处理可能出现的异常
        except Exception as e:

            raise mlp_exception.ConstructionError('DataCell')

    # 返回数据块ID方法
    def get_cell_id(self):

        # 返回数据块ID
        return self.__cell_id
    
    # 加载文件数据方法
    def __load_data(self):

        # 加载数据
        with pd.HDFStore(self.__docu_path) as store:

            # 读取数据
            self.__df_register = store[f'/Data/{self.__cell_id}']
    
    # 读取文件某列数据方法
    @typechecked
    def __load_column(self, var_name:str):

        # 打开文件
        with pd.HDFStore(self.__docu_path) as store:

            # 读取数据
            data = store[f'/Data/{self.__cell_id}'][var_name]
        
        # 返回数据
        return data

    # 保存文件数据方法
    def __save_data(self):

        # 保存数据
        with pd.HDFStore(self.__docu_path) as store:

            # 保存数据
            store[f'/Data/{self.__cell_id}'] = self.__df_register

    # 列追加数据方法
    @typechecked
    async def append_data(self,var_list:list[Variable]):

        # 将文档文件中数据加载至寄存DF对象
        await asyncio.to_thread(self.__load_data)
        
        # 遍历变量列表
        for var in var_list:

            # 如果新变量与现有变量名冲突
            if var.var_name in self.__var_dict.keys():

                # 如果原变量名没有(n)
                if not re.search(r'\(\d+\)$',var.var_name):

                    # DF对象中列名更改
                    var.df_register.rename(
                        columns = {var.var_name: var.var_name + '(1)'},
                        inplace = True
                    )
                    
                    # 将原变量名后添加(1)
                    var.var_name = f'{var.var_name}(1)'

                # 如果原变量名有(n)
                else:

                    # 获取原变量名(n)中的数字
                    num = int(
                        re.search(r'\(\d+\)$',var.var_name).group()[1:-1]
                    )

                    # 新变量名
                    new_var_name = var.var_name.replace(
                        f'{num}',
                        f'{num + 1}'
                    )

                    # DF对象中列名更改
                    var.df_register.rename(
                        columns = {var.var_name: new_var_name},
                        inplace = True
                    )

                    # 更改原变量名
                    var.var_name = new_var_name

            # 追加DF对象
            self.__df_register = pd.concat(
                [self.__df_register, var.df_register],
                axis = 1
            )

            # 保存变量对象
            self.__var_dict[var.var_name] = var
        
        # 保存文件数据
        self.__save_file()

        # 清空DF寄存对象
        self.__df_register = None

    # 数据查询方法
    @typechecked
    async def query_data(self,var_str:str):

        # 获取变量名
        var_name = var_str.split(':')[0]

        # 如果变量存在于变量字典
        if var_name in self.__var_dict.keys():

            # 读取对应列数据
            var_data = await asyncio.to_thread(self.__load_column(var_name))

            # 将数据打包为变量对象
            var = Variable(var_str)
            var.df_register = var_data

            # 返回变量对象
            return var
        
        # 若变量不存在于变量字典
        else:

            # 抛出数据查询异常
            raise mlp_exception.DataQueryError(var_name)

    # 数据替换方法
    @typechecked
    async def replace_data(self,var_list:list[Variable]):

        # 加载文件数据
        await self.__load_file()
        
        # 遍历变量列表
        for var in var_list:

            # 如果变量名存在于变量字典
            if var.var_name in self.__var_dict.keys():

                # 替换列数据
                self.__df_register[var.var_name] = var.df_register

            # 如果变量名不存在于变量字典
            else:

                # 抛出数据查询异常
                raise mlp_exception.DataQueryError(var.var_name)

        # 保存文件数据
        await self.__save_file()

    # 生成数据配置字典方法
    def gen_config(self):

        # 遍历变量字典，生成变量字符串列表
        var_str_list = []
        for var_name in self.__var_dict.keys():

            # 生成变量字符串
            var_str = self.__var_dict[var_name].gen_var_str()

            # 将变量字符串添加到变量字符串列表
            var_str_list.append(var_str)

        # 生成数据配置字典，并返回
        return {
            'cell_id': self.__cell_id,
            'var_list': var_str_list
        }
