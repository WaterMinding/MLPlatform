# 导入标准库模块
import os
import re
import time
from uuid import uuid4
from asyncio import to_thread

# 导入第三方库模块
import pandas as pd
from duckdb import connect

# 导入自定义模块
from .fifolock import pool_lock


# app包根路径
APP_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)


# 列名去重函数
def remove_dup(columns):

    length = len(columns)

    iterator = 0

    while iterator < length:

        name = columns[iterator]

        while name in columns[: iterator] or \
            name == 'CONNECT_ROW_ID':

            # 判断列名末尾是否包含(n)，其中n为正整数
            pattern  = r'\([1-9]\d*\)$'
            match_obj = re.search(
                pattern, 
                name
            )

            # 如果列名末尾不包含(n)
            if not match_obj:

                name += '(1)'

            # 如果列名末尾包含(n)
            else:

                n = int(
                    match_obj.group()[1:-1]
                )

                name = re.sub(
                    pattern,
                    f'({n+1})',
                    name
                )

        columns[iterator] = name

        iterator += 1
            

# 读取csv文件
async def read_csv(file):

    # 读取列名
    columns = pd.read_csv(
        file.file, 
        nrows = 1,
        header = None,
    ).to_numpy().tolist()[0]

    columns = list(
        map(str, columns)
    )

    await file.seek(0)

    # 读取数据
    data = await to_thread(
        pd.read_csv,
        filepath_or_buffer = file.file,
        skiprows = 1, 
        header = None,
        engine = "pyarrow" 
    )

    # 处理重复列名
    remove_dup(columns)

    # 设置新列名
    data.columns = columns

    # 关闭文件
    await file.close()

    return data


# 读取excel文件
async def read_excel(file):

    # 读取列名
    columns = pd.read_excel(
        file.file,
        nrows = 1,
        header = None,
    ).to_numpy().tolist()[0]

    columns = list(
        map(str, columns)
    )

    await file.seek(0)

    # 读取数据
    data = await to_thread(
        pd.read_excel,
        io = file.file,
        skiprows = 1,
        header = None,
        engine = 'calamine',
    )

    # 处理重复列名
    remove_dup(columns)

    # 设置新列名
    data.columns = columns

    # 关闭文件
    await file.close()

    return data
 

# 数据上传函数
async def upload_data(
    file,
    pool_path,
    pool_meta,
):
    
    #print('数据上传启动...')
    start = time.time()

    # 获取文件名与扩展名
    file_name = os.path.basename(file.filename)
    file_extension = os.path.splitext(file_name)[1]

    # 生成数据块ID
    random_id = uuid4().hex
    cell_id = f"data_{random_id}"

    # 读取数据
    if file_extension == ".xlsx":

        data = await read_excel(file)
    
    elif file_extension == ".csv":

        data = await read_csv(file)
    
    else:

        raise ValueError(
            f"暂不支持的文件类型 {file_extension}\n" +
            f"Unsupported file type {file_extension}"
        )
    
    # 获取列数据类型
    columns_types = list(
        data.dtypes.to_dict().values()
    )

    columns_types = list(
        map(str, columns_types)
    )

    iterator = 0
    length = len(columns_types)

    while iterator < length:

        if  'int' in columns_types[iterator] or \
            'float' in columns_types[iterator] or \
            'complex' in columns_types[iterator]:

            columns_types[iterator] = 'quan'
        
        else:

            columns_types[iterator] = 'cate'
        
        iterator += 1

    # 生成变量字符串列表
    cell_id_list = [cell_id] * len(columns_types)
    
    vars_config = list(
        zip(
            list(data.columns),
            columns_types,
            cell_id_list,
        )
    )

    var_str_list = [
        f"{var[0]}:{var[1]}:{var[2]}" 
        for var in vars_config
    ]
    
    # 生成长变量字符串
    long_var_str = ';'.join(var_str_list)

    # 保存数据
    async with pool_lock:

        with connect(pool_path) as conn:

            conn.register(
                f"{cell_id}",
                data
            )

            conn.sql(
                f"CREATE TABLE IF NOT EXISTS {cell_id} " +
                f"AS SELECT * FROM {cell_id}"
            )

            conn.sql(
                f"INSERT INTO {pool_meta} " +
                f"(cell_id, cell_name, variables) " +
                f"VALUES ('{cell_id}','{file_name}'," +
                f"'{long_var_str}')"
            )

            # 获取新数据块的元数据
            new_meta = conn.sql(
                f"SELECT * FROM {pool_meta} " + 
                f"WHERE cell_id = '{cell_id}'"
            ).df()
    
    end = time.time()
    #print('数据保存完成\n',f"{end - start} 秒")

    return new_meta.values[0].tolist()