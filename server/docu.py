# 导包
import os
import time
import base64
from io import BytesIO
from exception import ParamTypeException
from exception import PathNotExistsException
from exception import RedundantOperationException


# 类型检查函数
def type_check(var,var_name:str,correct_type:type):
    
    if type(var) != correct_type:

        raise ParamTypeException(str(var_name) + '应当为' + str(correct_type))


# 定义 “块” 类
class Cell:

    # 构造方法
    def __init__(self,cell_id:str,cell_type:str):

        # 检查参数类型
        type_check(cell_id,'cell_id',str)
        type_check(cell_type,'cell_type',str)
        
        # 定义块属性：块ID
        self.__id = cell_id

        # 定义块属性：块类型
        self.__type = cell_type

        # 定义块属性：元数据词典
        self.__meta_dict = {}

    # 返回块ID方法
    def get_cell_id(self) -> str:
        return self.__id

    # 返回块类型方法
    def get_cell_type(self) -> str:
        return self.__type
    
    # 覆盖元数据词典方法
    def set_meta(self,new_meta_dict):

        self.__meta_dict = new_meta_dict
    
    # 元数据词典新增数据方法
    def add_meta(self,additive_meta_dict):

        self.__meta_dict.update(additive_meta_dict)
    
    # 元数据词典删除数据方法
    def delete_meta(self,delete_key):

        if delete_key in self.__meta_dict.keys():
            self.__meta_dict.pop(delete_key)
    
    # 查询元数据方法
    def query_meta(self,query_key):

        if query_key in self.__meta_dict.keys():
            return self.__meta_dict[query_key]
        else:
            return None

    # 返回元数据词典方法
    def get_meta(self):

        return self.__meta_dict


# 定义文本块类（继承Cell类）
class TextCell(Cell):

    # 构造方法
    def __init__(self,docu_name:str):

        # 检查参数类型
        type_check(docu_name,'docu_name',str)

        cell_id = docu_name + 'Text' + str(time.time())
        cell_type = 'Text'

        # 构造父类对象
        super().__init__(cell_id,cell_type)

        # 定义文本块属性：文本字符串
        self.__text = None

        # 删除元数据字典属性
        self.__meta_dict = None
    
    # 覆盖文本字符串方法
    def set_text(self,new_text:str):

        # 检查参数类型
        type_check(new_text,'new_text',str)

        # 覆盖文本字符串
        self.__text = new_text
    
    # 返回文本字符串方法
    def get_text(self) -> str:

        return self.__text


# 定义图像块类
class ChartCell(Cell):

    # 构造方法
    def __init__(self, docu_name:str):

        # 检查参数类型
        type_check(docu_name,'docu_name',str)

        cell_id = docu_name + 'Chart' + str(time.time())
        cell_type = 'Chart'

        # 构造父类对象
        super().__init__(cell_id,cell_type)


# 定义算子块类
class OperatorCell(Cell):

    # 构造方法
    def __init__(self, docu_name:str):

        # 检查参数类型
        type_check(docu_name,'docu_name',str)

        cell_id = docu_name + 'Operator' + str(time.time())
        cell_type = 'Operator'

        # 构造父类对象
        super().__init__(cell_id,cell_type)


# 定义数据块类
class OperatorCell(Cell):

    # 构造方法
    def __init__(self, docu_name:str):

        # 检查参数类型
        type_check(docu_name,'docu_name',str)

        cell_id = docu_name + 'Data' + str(time.time())
        cell_type = 'Data'

        # 构造父类对象
        super().__init__(cell_id,cell_type)

        # 定义数据块属性：Dataframe对象引用
        self.__df = None
    
    # 覆盖DataFrame引用方法
    def set_df(self,new_df):

        self.__df = new_df
    
    # 返回DataFrame引用方法
    def get_df_pointer(self):

        return self.__df
    
    # 获取DataFrame对象的复制体
    def get_df_copy(self):

        return self.__df.copy()
    

