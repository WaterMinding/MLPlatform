# 导入标准库模块
import os
from copy import deepcopy

# 导入第三方库模块
from typeguard import typechecked

# 导入自定义库模块
from .datacell import DataCell
from .datacell import DataConfig
from ..mlp_exceptions import ConstructionError


# 定义运行时数据池类
class DataPool:

    # 构造方法
    # 参数1：pool_path - 数据池文件路径
    # 参数2：pool_config - 数据池配置
    @typechecked
    def __init__(
        self, 
        pool_path: str,
        pool_config: dict[str, DataConfig] | None = None
    ):
        
        # 保存数据池文件路径
        self.__pool_path = pool_path

        # 检查数据池文件路径是否合法
        flag = os.path.isfile(self.pool_path)

        if not flag:
            
            raise FileNotFoundError(
                self.pool_path
            ) 
        
        # 定义数据块字典
        self.__cell_dict = {}

        # 初始化数据块字典
        try:

            # 如果配置不为空
            if pool_config is not None:

                # 遍历配置
                for cell_id in pool_config:

                    # 添加数据块
                    self.add_cell(
                        data_config = pool_config[cell_id]
                    )

        except Exception as e:

            raise ConstructionError('DataPool')

    # 获取数据池文件路径
    @property
    def pool_path(self):

        return self.__pool_path
    
    # 获取数据块字典
    @property
    def cell_dict(self):

        return deepcopy(self.__cell_dict)

    # 添加数据块方法
    # 参数1：data_config - 数据块配置
    @typechecked
    def add_cell(self, data_config: DataConfig):
        
        # 构造数据块
        cell = DataCell(
            pool_path = self.__pool_path,
            data_config = data_config
        )

        # 保存数据块
        self.__cell_dict[cell.cell_id] = cell
    
    # 获取数据块方法
    # 参数1：cell_id - 数据块ID
    @typechecked
    def get_cell(self, cell_id: str) -> DataCell:

        # 返回数据块
        return self.__cell_dict[cell_id]
    
    # 获取数据池配置方法
    def get_config(self) -> dict:

        # 构造数据池配置
        pool_config = {
            cell_id: cell.get_config() \
            for cell_id, cell in self.__cell_dict.items()
        }

        # 返回数据池配置
        return pool_config
