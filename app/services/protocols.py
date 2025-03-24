# 导入标准库模块
from typing import TypedDict


# 文档配置字典协议
class DocuConfig(TypedDict):

    # 文档名称
    docu_name: str
    
    # 编辑区列表
    edit_list: list[dict]

    # 数据区列表
    data_list: list[dict]
