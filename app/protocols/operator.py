# 导入标准库模块
from typing import Protocol
from typing import runtime_checkable

# 导入自定义模块
from ..protocols import OpResult


# 定义算子类协议
@runtime_checkable
class Operator(Protocol):

    def __init__(
        self, 
        params: dict | None, 
        variables: dict | None
    ):
        ... # pragma: no cover

    def run(*args, **kwargs) -> OpResult:

        ... # pragma: no cover

    def check(*args, **kwargs):

        ... # pragma: no cover