# 导入标准库模块

# 导入第三方库模块

# 导入自定义模块
from ..data import DataPool 
from .fifolock import pool_lock
from ..operators import op_entry
from ..plotters import plotter_entry
from ..protocols import OpConfig
from ..protocols import OpConfigFront
from ..protocols import OpResult
from ..protocols import VarUsage
from ..mlp_exceptions import DocuNotFoundError
from ..mlp_exceptions import VarUsageNotFoundError


# 算子执行函数
async def run_op(
    op_config: OpConfigFront,
    data_pool: DataPool
):
    
    if data_pool is None:

        raise DocuNotFoundError()

    op_config = OpConfig(
        cell_type = op_config.cell_type, 
        op_name = op_config.op_name,
        parameters = op_config.parameters,
        variables = op_config.variables
    )

    variables = op_config.variables

    # 变量字符串 -> 变量对象
    for area in variables.values():

        area_size = len(area)
        iterator = 0

        while iterator < area_size:

            var_str = area[iterator]
            var_pack = var_str.split(':')
            var_name = var_pack[0]
            owner_id = var_pack[2]

            data_cell = data_pool.get_cell(
                owner_id, 
            )

            try:
                
                var = data_cell.get_var(var_name)
            
            except Exception as e:
                
                raise e

            area[iterator] = var

            iterator += 1
    
    # 执行算子
    try: 
        
        op_result: OpResult = op_entry(op_config)
    
    except Exception as e:

        raise e
    
    # 解包结果
    chart_list = op_result.chart_list
    data_list = op_result.data_list

    # 处理图像区
    if chart_list is not None:
    
        chart_list_size = len(chart_list)
        iterator = 0

        while iterator < chart_list_size:

            chart_config = chart_list[iterator]
            
            image_config = plotter_entry(chart_config)
            
            chart_list[iterator] = image_config

            iterator += 1
        
    # 处理数据区
    if data_list is not None:

        data_list_size = len(data_list)
        iterator = 0

        async with pool_lock:

            # 对于每个变量，根据用途调用其所属数据块对应方法
            while iterator < data_list_size:

                var = data_list[iterator]
                
                data_cell = data_pool.get_cell(
                    var.owner_id
                )
                
                if var.usage == VarUsage.REPLACE:

                    data_cell.replace_var([var])

                elif var.usage == VarUsage.APPEND:

                    data_cell.append_var([var])
            
                else:

                    raise VarUsageNotFoundError(
                        var.usage
                    ) 
                
                new_data_config = data_cell.get_config()

                # 将数据区的变量替换为数据块的新配置
                data_list[iterator] = new_data_config

                iterator += 1
    
    return op_result
        



            


