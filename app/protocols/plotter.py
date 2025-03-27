# 导入标准库模块
from typing import Protocol
from typing import runtime_checkable

# 导入第三方库模块
from matplotlib.axes import Axes

# 导入自定义模块
from .cells_config import ElemConfig


# 定义绘图类协议
@runtime_checkable
class Plotter(Protocol):

    def __init__(self, elem_config: ElemConfig) -> None:

        ... # pragma: no cover

    def plot(self, ax: Axes) -> None:

        ... # pragma: no cover
    
    def check_params(self) -> bool:

        ... # pragma: no cover