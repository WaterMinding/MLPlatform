# 导入标准库模块
from typing import TypedDict
from typing import Protocol
from typing import runtime_checkable

# 导入第三方库模块
from pandas import DataFrame as DF

# 导入自定义模块
from ..data import Variable


# 定义文本配置字典协议
class TextConfig(TypedDict):

    cell_num: int
    text: str


# 定义元素配置字典协议
class ElemConfig(TypedDict):

    elem_type: str
    params: DF


# 定义图像配置字典协议
class ChartConfig(TypedDict):

    cell_num: int
    elem_list: list[ElemConfig]


# 定义算子层结果字典
class OPResult(TypedDict):

    text_list: list[TextConfig] | None
    chart_list: list[ChartConfig] | None
    data_list: list[Variable] | None


# 定义算子类协议
@runtime_checkable
class Operator(Protocol):

    def __init__(
        self, 
        params: dict | None, 
        variables: dict | None
    ):
        ... # pragma: no cover

    def run(*args,**kwargs) -> OPResult:

        ... # pragma: no cover