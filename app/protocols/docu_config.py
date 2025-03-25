# 导入标准库模块
from typing import Union

# 导入第三方库模块
from pydantic import BaseModel

# 导入自定义模块
from .typed_dicts import DataConfig
from .typed_dicts import TextConfig
from .typed_dicts import ChartConfig
from .typed_dicts import OpConfig


# 文档配置模型
class DocuConfig(BaseModel):

    docu_name: str

    edit_list: list[
        Union[
            TextConfig, 
            ChartConfig, 
            OpConfig
        ]
    ]

    data_list: list[DataConfig]

    class Config:

        # Pydantic无法严格校验DataFrame
        arbitrary_types_allowed = True


    