# 导入标准库
import os
import asyncio
from uuid import uuid4

# 导入第三方库
import h5py
from typeguard import typechecked

# 导入自定义模块
from cells import TextCell
from cells import OpCell
from cells import Element
from cells import ChartCell
from cells import Variable
from cells import DataCell
from mlp_exception import CellTypeError
from mlp_exception import FilePathError
from mlp_exception import CellQueryError
from mlp_exception import ConstructionError


# 确定脚本路径
FILE_PATH = os.path.abspath(__file__)


# 文档类
class Docu:

    # 构造方法
    @typechecked
    def __init__(
        self,
        docu_name:str,
        docu_path:str,
        mode:str
    ):

        # 保存文档名
        self.__docu_name = docu_name

        # 确定数据缓存区路径
        cache_path = FILE_PATH.replace(
            "docu.py", 
            "data_cache"
        )

        # 如果构造模式是"new"
        if mode == "new":

            # 构造文件路径
            file_path = f'{cache_path}/{docu_name}.docu'

            # 生成文档文件
            with h5py.File(
                file_path,
                mode = "w"
            ) as file:
                
                # 构造数据数据组
                file.create_group(
                    name = "Data"
                )

                # 构造图像数据组
                file.create_group(
                    name = "Chart"
                )

            # 保存文档路径
            self.__docu_path = file_path
        
        # 如果构造模式是"open"
        elif mode == "open":

            # 检查文件扩展名
            flag = docu_path.split(".")[-1] == "docu"

            # 检查路径是否文件
            flag = flag and os.path.isfile(
                docu_path
            )

            # 抛出异常
            if not flag:
                raise FilePathError(
                    file_path = docu_path
                )

            # 保存文档路径
            self.__docu_path = docu_path
        
        # 如果构造模式不是"new"或"open"
        else:

            # 抛出异常
            raise ConstructionError(
                elem_type = "Docu"
            )
        
        # 定义编辑区列表
        self.__edit_area = []

        # 定义数据块字典
        self.__data_cells = {}

    # 获取块方法
    @typechecked
    def get_cell(self, cell_id:str):
        
        # 读取块类型
        cell_type = cell_id.split(":")[0]

        # 如果是数据块
        if cell_type == "data":

            # 返回数据块
            return self.__data_cells[cell_id]

        # 如果不是数据块
        else:

            # 调用获取编辑区块位置方法
            try:
                cell_loc = self.get_cell_loc(cell_id)
            except CellQueryError as e:
                raise e
            
            # 取块并返回
            return self.__edit_area[cell_loc]

    # 获取块位置方法
    @typechecked
    def get_cell_loc(self, cell_id:str):

        flag = False

        # 获取编辑区列表长度
        length = len(self.__edit_area)

        # 遍历编辑区列表
        for i in range(length):

            # 如果块ID匹配
            if self.__edit_area[i].get_cell_id() == cell_id:

                # 调整标志
                flag = True

                # 跳出循环
                break

        # 如果匹配
        if flag:

            # 返回块位置
            return i

        # 如果不匹配
        else:

            # 抛出异常
            raise CellQueryError(
                cell_id = cell_id
            )

    # 添加数据块方法
    @typechecked
    def add_data_cell(self, data_config:dict):

        # 添加数据块到数据块字典
        self.__data_cells[
            data_config['cell_id']
        ] = DataCell(
            data_config,
            self.__docu_path
        )

    # 添加编辑区块方法
    @typechecked
    def add_edit_area_cell(
        self,
        config:dict,
        cell_type:str,
        target_loc:int|None = None
    ):
        
        # 如果配置字典中ID为空
        if config['cell_id'] == None:

            # 生成块ID
            uu_id = str(uuid4())
            cell_id = \
            f'{cell_type}:{self.__docu_name}:{uu_id}'

            print(cell_id)

            # 更新配置字典
            config['cell_id'] = cell_id

        # 如果为图像块
        if cell_type == 'chart':

            # 生成图像块
            cell = ChartCell(
                config,
                self.__docu_path
            )
        
        # 如果为文本块
        elif cell_type == 'text':

            # 生成文本块
            cell = TextCell(
                config,
            )
        
        # 如果为算子块
        elif cell_type == 'op':

            # 生成算子块
            cell = OpCell(
                config,
            )
        
        # 如果为其他
        else:

            # 抛出异常
            raise CellTypeError(
                cell_type
            )

        # 如果目标位置为None
        if target_loc == None:

            # 将图像块添加到编辑区列表末尾
            self.__edit_area.append(cell)

        # 否则
        else:

            # 将图像块添加到编辑区列表指定位置
            self.__edit_area.insert(
                target_loc,
                cell
            )

        # 返回图像块ID
        return config['cell_id']

    # 块位置浮动方法
    @typechecked
    def cell_loc_vary(
        self,
        cell_id:str,
        direction:int
    ):
        
        # 获取块位置
        try:
            cell_loc = self.get_cell_loc(cell_id)
        except CellQueryError as e:
            raise e
        
        # 如果方向为0
        if direction == 0:

            # 如果块位置不为0
            if cell_loc != 0:

                # 计算新位置
                new_loc = cell_loc - 1

                # 弹出块
                cell = self.__edit_area.pop(
                    cell_loc
                )

                # 将块插入到新位置
                self.__edit_area.insert(
                    new_loc,
                    cell
                )

            # 否则
            else:
                new_loc = cell_loc

        # 如果方向为1
        elif direction == 1:

            # 如果块位置不为编辑区列表长度减1
            if cell_loc != len(self.__edit_area) - 1:

                # 计算新位置
                new_loc = cell_loc + 1

                # 弹出块
                cell = self.__edit_area.pop(
                    cell_loc
                )

                # 将块插入到新位置
                self.__edit_area.insert(
                    new_loc,
                    cell
                )

            # 否则
            else:
                new_loc = cell_loc

        # 返回新位置
        return new_loc
    
    # 删除块
    @typechecked
    def remove_cell(self, cell_id:str):

        





        
            

        





