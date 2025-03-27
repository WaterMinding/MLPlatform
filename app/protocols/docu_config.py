# 导入标准库模块
from typing import Union

# 导入第三方库模块
from pydantic import BaseModel

# 导入自定义模块
from .cells_config import DataConfig
from .cells_config import TextConfig
from .cells_config import ImageConfig
from .cells_config import OpConfig


# 文档配置模型
class DocuConfig(BaseModel):

    docu_name: str

    edit_list: list[
        Union[
            TextConfig, 
            ImageConfig, 
            OpConfig
        ]
    ]

    data_list: list[DataConfig]

    class Config:

        # Pydantic无法严格校验DataFrame
        arbitrary_types_allowed = True


    