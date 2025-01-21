# 导包
import os
import time
import base64
from io import BytesIO
from pandas import DataFrame as DF
from exception import ParamTypeException
from exception import PathNotExistsException
from exception import RedundantOperationException
from exception import ObjectNotExistsException


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

        # 归属集合
        self.__belong_set = set()

        # 定义块属性：元数据词典
        self.__meta_dict = {}

    # 返回块ID方法
    def get_cell_id(self) -> str:
        return self.__id

    # 返回块类型方法
    def get_cell_type(self) -> str:
        return self.__type
    
    # 覆盖元数据词典方法
    def set_meta(self,new_meta_dict:dict):

        self.__meta_dict = new_meta_dict
    
    # 元数据词典新增数据方法
    def add_meta(self,additive_meta_dict:dict):

        self.__meta_dict.update(additive_meta_dict)
    
    # 元数据词典删除数据方法
    def delete_meta(self,delete_key):

        if delete_key in self.__meta_dict.keys():
            return self.__meta_dict.pop(delete_key)
    
    # 查询元数据方法
    def query_meta(self,query_key):

        if query_key in self.__meta_dict.keys():
            return self.__meta_dict[query_key]
        else:
            return None

    # 返回元数据字典方法
    def get_meta(self):

        return self.__meta_dict
    
    # 获取归属集合方法
    def get_belong_set(self):

        return self.__belong_set
    
    # 覆盖归属集合方法
    def set_belong_set(self,new_set):

        self.__belong_set = new_set
    
    # 归属集合增加方法
    def add_belong_set(self,new_belong):

        self.__belong_set.add(new_belong)
    
    # 归属集合删除方法
    def remove_belong_set(self,target_belong):

        if target_belong in self.__belong_set:

            self.__belong_set.remove(target_belong)
    
    # 归属列表清空方法
    def clr_belong_set(self):

        self.__belong_set.clear()
        


# 定义文本块类（继承Cell类）
class TextCell(Cell):

    # 构造方法
    def __init__(self,docu_id:str):

        # 检查参数类型
        type_check(docu_id,'docu_id',str)

        cell_id = docu_id + 'Text' + str(time.time())
        cell_type = 'Text'

        # 构造父类对象
        super().__init__(cell_id,cell_type)

        # 定义文本块属性：文本字符串
        self.__text = None
    
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
    def __init__(self, docu_id:str):

        # 检查参数类型
        type_check(docu_id,'docu_id',str)

        cell_id = docu_id + 'Chart' + str(time.time())
        cell_type = 'Chart'

        # 构造父类对象
        super().__init__(cell_id,cell_type)


# 定义算子块类
class OperatorCell(Cell):

    # 构造方法
    def __init__(self, docu_id:str):

        # 检查参数类型
        type_check(docu_id,'docu_id',str)

        cell_id = docu_id + 'Operator' + str(time.time())
        cell_type = 'Operator'

        # 构造父类对象
        super().__init__(cell_id,cell_type)


# 定义数据块类
class DataCell(Cell):

    # 构造方法
    def __init__(self, docu_id:str):

        # 检查参数类型
        type_check(docu_id,'docu_id',str)

        cell_id = docu_id + 'Data' + str(time.time())
        cell_type = 'Data'

        # 构造父类对象
        super().__init__(cell_id,cell_type)

        # 定义数据块属性：Dataframe对象引用
        self.__df = None
    
    # 覆盖DataFrame引用方法
    def set_df(self,new_df:DF):

        self.__df = new_df
    
    # 返回DataFrame引用方法
    def get_df_pointer(self):

        return self.__df
    
    # 获取DataFrame对象的复制体
    def get_df_copy(self):

        return self.__df.copy()


# 定义文档类
class Docu:

    # 构造方法
    def __init__(self,docu_name:str):

        # 检查参数类型
        type_check(docu_name,'docu_name',str)
        
        # 定义文档类属性：文档名
        self.__docu_name = docu_name

        # 定义文档类属性：文档ID
        self.__docu_id = self.__docu_name + str(time.time())
        
        # 定义文档类属性：块索引列表
        self.__cell_list = []

        # 定义文档类属性：数据块池
        self.__data_pool = []
    
    # 类方法：按照ID查找某个块列表中的块
    @classmethod
    def find_cell(cls,cell_id,cell_lst):

        # 检查参数类型
        type_check(cell_id,'cell_id',str)
        type_check(cell_lst,'cell_lst',list)

        # 设置块存在标志
        flag = False

        # 初始化now_loc
        now_loc = None

        # 寻找块
        for now_loc in range(len(cell_lst)):

            if cell_id == cell_lst[now_loc].get_cell_id():
                flag = True
                break
        
        # 返回寻找结果
        return flag,now_loc

    # 返回文档名方法
    def get_docu_name(self):

        return self.__docu_name
    
    # 返回文档id方法
    def get_docu_id(self):

        return self.__docu_id
    
    # 返回块索引列表方法
    def get_cell_list(self):

        return self.__cell_list
    
    # 块索引列表增加块方法
    def add_cell_to_list(self,cell):

        cell.add_belong_set(self,"cell_list")

        self.__cell_list.append(cell)
    
    # 块索引列表删除块方法
    def delete_cell_from_list(self,cell_id:str):

        # 参数类型检查
        type_check(cell_id,'cell_id',str)

        # 寻找块位置
        flag,now_loc = Docu.find_cell(cell_id,self.__cell_list)
        
        # 确定是否删除了某个块
        if flag:

            # 如果找到了块，则删除块
            deleted_cell = self.__cell_list.pop(now_loc)
            deleted_cell.remove_belong_set("cell_list")

        else:
            deleted_cell = None
            raise ObjectNotExistsException(str(cell_id) + '不存在于块索引列表')
    
        # 返回被删除项
        return deleted_cell

    # 块索引列表调整块位置方法
    def arrange_loc_in_list(self,cell_id:str,new_loc:int):

        # 参数类型检查
        type_check(cell_id,'cell_id',str)
        type_check(new_loc,'new_loc',int)

        # 寻找块位置
        flag,now_loc = Docu.find_cell(cell_id,self.__cell_list)
        
        # 从列表中弹出所选块并插入新位置
        if flag:
            now_cell = self.__cell_list.pop(now_loc)
            self.__cell_list.insert(new_loc,now_cell)
        else:
            raise ObjectNotExistsException(str(cell_id) + '不存在于块索引列表')
    
    # 块索引列表通过ID检索块方法
    def get_cell_from_list(self,cell_id:str):

        # 参数类型检查
        type_check(cell_id,'cell_id',str)

        # 寻找块位置
        flag,now_loc = Docu.find_cell(cell_id,self.__cell_list)

        # 获取块
        if flag:
            now_cell = self.__cell_list[now_loc]
        else:
            now_cell = None
            raise ObjectNotExistsException(str(cell_id) + '不存在于块索引列表')
        
        # 返回块
        return now_cell
    
    # 返回数据块池方法
    def get_data_pool(self):

        return self.__data_pool
    
    # 数据块池增加块方法
    def add_cell_to_data_pool(self,cell):

        # 判断数据块池中是否已经存在该块
        flag,now_loc = Docu.find_cell(cell.get_cell_id(),self.__data_pool)

        if not flag:
            self.__data_pool.append(cell)
            cell.add_belong_set("data_pool")
        else:
            raise RedundantOperationException(str(cell.get_cell_id()) + '已经存在于数据块池，勿重复添加')

    # 数据块池删除块方法
    def delete_cell_from_data_pool(self,cell_id):

        # 判断数据块池中是否存在该块
        flag,now_loc = Docu.find_cell(cell_id,self.__data_pool)

        # 删除块
        if flag:
            now_cell = self.__data_pool.pop(now_loc)
            now_cell.remove_belong_set("data_pool")
        else:
            now_cell = None
            raise ObjectNotExistsException(str(cell_id) + '不存在于数据块池')

        # 返回被删除块
        return now_cell

    # 数据池ID检索块方法
    def get_cell_from_data_pool(self,cell_id):

        # 查找目标块位置
        flag,now_loc = Docu.find_cell(cell_id,self.__data_pool)

        # 获取目标块
        if flag:
            now_cell = self.__data_pool[now_loc]
        else:
            now_cell = None
            raise ObjectNotExistsException(str(cell_id) + '不存在于数据块池')
        
        # 返回目标块
        return now_cell