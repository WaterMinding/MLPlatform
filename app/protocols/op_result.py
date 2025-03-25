# 导入第三方库模块
from pydantic import BaseModel

# 导入自定义模块
from ..data import Variable
from .typed_dicts import TextConfig
from .typed_dicts import ChartConfig


# 定义算子层结果配置
class OpResult(BaseModel):

    text_list: list[TextConfig] | None
    chart_list: list[ChartConfig] | None
    data_list: list[Variable] | None

    class Config:

        # Pydantic无法严格校验Variables
        arbitrary_types_allowed = True