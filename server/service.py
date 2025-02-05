import json
import asyncio
import docu
from docu import Docu,Cell
from exception import ObjectNotExistsException
from exception import ParamTypeException

# 垃圾桶
trash_bin = []

# 新建文档
async def create_docu(docu_name:str):

    # 创建文档
    document = Docu(docu_name)

    return document



# 保存文档
async def save_docu(document:Docu,):

    pass


# 打开文档
async def open_docu():

    pass


# 增加块（块列表）
async def add_cell(document:Docu,cell_type:str,meta_dict:dict = None) -> Cell:

    # 检查文档对象
    if document == None:

        raise ObjectNotExistsException("文档不存在")

    # 创建块
    if cell_type == "Text":
        cell = docu.TextCell(document.get_docu_id())
    elif cell_type == "Operator":
        cell = docu.OperatorCell(document.get_docu_id())
    elif cell_type == "Data":
        cell = docu.DataCell(document.get_docu_id())
    elif cell_type == "Chart":
        cell = docu.ChartCell(document.get_docu_id())
    else:
        raise ParamTypeException("块类型不正确")
    
    # 添加块
    document.add_cell_to_list(cell)

    # 如果传入了元数据字典，则赋给新块
    cell.set_meta(meta_dict)

    # 返回块ID
    return cell


# 删除块（块列表）
async def remove_cell(document:Docu,cell_id:str) -> Cell:

    # 检查文档对象
    if document == None:

        raise ObjectNotExistsException("文档不存在")
    
    # 删除块
    try:
        cell = document.delete_cell_from_list(cell_id)
    except Exception as e:
        return None
    
    return cell


# 排列块
async def arrange_cell_loc(document:Docu,cell_id:str,new_loc:int) -> int:

    # 检查文档对象
    if document == None:

        raise ObjectNotExistsException("文档不存在")

    # 重置位置
    try:
        document.arrange_loc_in_list(cell_id,new_loc)
    except:
        return None
    
    return new_loc

# 编辑块
async def edit_cell():

    pass


# 调用算子块
async def use_op():

    pass


# 上传数据
async def upload_data():

    pass