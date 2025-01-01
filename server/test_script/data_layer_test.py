import docu
from docu import TextCell as TC
from docu import OperatorCell as OC
from docu import DataCell as DC
from docu import ChartCell as CC
from docu import Docu
from pandas import DataFrame as DF
import numpy as np

# 创建文档对象
print("\n\n\n创建文档对象\n\n")
doc = Docu('数据分析报告')

# 测试文档对象返回文档名方法
print("\n\n\n测试文档对象返回文档名方法\n\n")
print(doc.get_docu_name())

# 测试文档对象返回文档ID方法
print("\n\n\n测试文档对象返回文档ID方法\n\n")
print(doc.get_docu_id())

# 创建四种块对象
print("\n\n\n创建四种块对象\n\n")
tc = TC(doc.get_docu_id())
oc = OC(doc.get_docu_id())
dc = DC(doc.get_docu_id())
cc = CC(doc.get_docu_id())

# 测试块对象返回块ID方法
print("\n\n\n测试块对象返回块ID方法\n\n")
print(cc.get_cell_id())

# 测试块对象返回块类型方法
print("\n\n\n测试块对象返回块类型方法\n\n")
print(cc.get_cell_type())

# 测试块对象覆盖元数据词典方法
print("\n\n\n测试块对象覆盖元数据词典方法\n\n")
cc.set_meta({'x':[1,2,3,4,5],'y':[1,4,9,16,25]})
print(cc.get_meta())

# 测试块对象元词典新增数据方法
print("\n\n\n测试块对象元词典新增数据方法\n\n")
cc.add_meta({'h':5})
print(cc.get_meta())

# 测试块对象删除元数据方法
print("\n\n\n测试块对象删除元数据方法\n\n")
cc.delete_meta('x')
print(cc.get_meta())

# 测试块对象查询元数据方法
print("\n\n\n测试块对象查询元数据方法\n\n")
print(cc.query_meta('y'))

# 测试文本块覆盖文本字符串方法
print("\n\n\n测试文本块覆盖文本字符串方法\n\n")
tc.set_text('伸手摘星，即使徒劳无功也不致一手污泥。')

# 测试返回文本字符串方法
print("\n\n\n测试返回文本字符串方法\n\n")
print(tc.get_text())

# 测试数据块覆盖DafaFrame方法
print("\n\n\n测试数据块覆盖DafaFrame方法\n\n")
dc.set_df(DF(np.array([[1,2,3],[4,5,6],[7,8,9]])))
print(dc.get_df_pointer())

# 测试数据块获得DataFrame对象的复制体的方法
print("\n\n\n测试数据块获得DataFrame对象的复制体的方法\n\n")
print(dc.get_df_copy())

# 测试文档类块索引列表增加块方法
print("\n\n\n测试文档类块索引列表增加块方法\n\n")
doc.add_cell_to_list(tc)
doc.add_cell_to_list(oc)
doc.add_cell_to_list(dc)
doc.add_cell_to_list(cc)
print(doc.get_cell_list())

# 测试文档类块索引列表删除块方法
print("\n\n\n测试文档类块索引列表删除块方法\n\n")
doc.delete_cell_from_list(oc.get_cell_id())
print(doc.get_cell_list())

# 测试文档类块索引列表调整块位置方法
print("\n\n\n测试文档类块索引列表调整块位置方法\n\n")
doc.arrange_loc_in_list(tc.get_cell_id(),2)
print(doc.get_cell_list())

# 测试文档类通过ID检索块方法
print("\n\n\n测试文档类通过ID检索块方法\n\n")
print(doc.get_cell_from_list(tc.get_cell_id()))

###########################
# 测试文档类新增算子池增加块方法
print("\n\n\n测试文档类新增算子池增加块方法\n\n")
doc.add_cell_to_op_pool(oc)
print(doc.get_new_op_pool())

# 测试文档类新增算子池检索块方法
print("\n\n\n测试文档类新增算子池检索块方法\n\n")
print(doc.get_cell_from_op_pool(oc.get_cell_id()))

# 测试文档类新增算子池删除块方法
print("\n\n\n测试文档类新增算子池删除块方法\n\n")
print(doc.delete_cell_from_op_pool(oc.get_cell_id()))

# 测试文档类数据池增加块方法
print("\n\n\n测试文档类数据池增加块方法\n\n")
doc.add_cell_to_data_pool(dc)
print(doc.get_data_pool())

# 测试文档类数据池检索块方法
print("\n\n\n测试文档类数据池检索块方法\n\n")
print(doc.get_cell_from_data_pool(dc.get_cell_id()))

# 测试文档类数据池删除块方法
print("\n\n\n测试文档类数据池删除块方法\n\n")
print(doc.delete_cell_from_data_pool(dc.get_cell_id()))
print(doc.get_data_pool())
