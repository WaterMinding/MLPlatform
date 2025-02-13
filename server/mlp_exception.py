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