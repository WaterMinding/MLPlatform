# 导入标准库模块
from enum import Enum

# 导入第三方库模块
from pydantic import BaseModel
from pandas import DataFrame as DF

# 导入自定义模块
from ..data import Variable

# 定义数据配置协议
# 为避免与data包的循环依赖，
# 这里将协议定义在data.datacell中
from ..data.datacell import DataConfig

class CellType(Enum):

    TEXT = 'text'
    CHART = 'chart'
    OP = 'op'
    IMAGE = 'image'


# 定义文本配置模型
class TextConfig(BaseModel):

    cell_type: CellType
    cell_num: int
    text: str


# 定义元素配置模型
class ElemConfig(BaseModel):

    elem_type: str
    params: DF

    class Config:

        # Pydantic无法严格校验DataFrame
        arbitrary_types_allowed = True


# 定义图像配置模型
class ChartConfig(BaseModel):

    cell_type: CellType
    cell_num: int
    elem_list: list[ElemConfig]


# 定义已绘制图像配置模型
class ImageConfig(BaseModel):

    cell_type: CellType
    image: str


# 定义算子配置
class OpConfig(BaseModel):

    cell_type: CellType
    op_name: str
    parameters: dict | None
    variables: dict | None

    class Config:

        # Pydantic无法严格校验Variables
        arbitrary_types_allowed = True