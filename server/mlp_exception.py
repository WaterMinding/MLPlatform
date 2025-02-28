# 构造异常
class ConstructionError(Exception):

    # 异常构造方法
    def __init__(self, elem_type):

        # 定义错误信息
        self.error_message = f"{elem_type}对象构造失败\n"
        self.error_message += f"{elem_type} construction failed"
        
        # 调用父类构造方法，并传递错误信息
        super().__init__(self.error_message)


# 数据查询异常
class DataQueryError(Exception):

    # 异常构造方法
    def __init__(self,var_name):

        # 定义错误信息
        self.error_message = f"{var_name}数据查询失败\n"
        self.error_message += f"{var_name} data query failed"
        
        # 调用父类构造方法，并传递错误信息
        super().__init__(self.error_message)


# 文件路径异常
class FilePathError(Exception):

    # 异常构造方法
    def __init__(self, file_path):

        # 定义错误信息
        self.error_message = f"{file_path}文件路径错误\n"
        self.error_message += f"{file_path} file path error"

        # 调用父类构造方法，并传递错误信息
        super().__init__(self.error_message)


# 块查询异常
class CellQueryError(Exception):

    # 异常构造方法
    def __init__(self, cell_id):

        # 定义错误信息
        self.error_message = f"{cell_id}块查询失败\n"
        self.error_message += f"{cell_id} cell query failed"

        # 调用父类构造方法，并传递错误信息
        super().__init__(self.error_message)


# 块类型异常
class CellTypeError(Exception):

    # 异常构造方法
    def __init__(self, cell_type):

        # 定义错误信息
        self.error_message = f"{cell_type}块类型错误\n"
        self.error_message += f"{cell_type} cell type error"

        # 调用父类构造方法，并传递错误信息
        super().__init__(self.error_message)