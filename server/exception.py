# 参数类型异常
class ParamTypeException(Exception):

    # 构造方法
    def __init__(self, message:str):
        self.message = message
        super().__init__(self.message)


# 重复操作异常
class RedundantOperationException(Exception):

    # 构造方法
    def __init__(self, message:str):
        self.message = message
        super().__init__(self.message)


# 路径不存在异常
class PathNotExistsException(Exception):

    # 构造方法
    def __init__(self, message:str):
        self.message = message
        super().__init__(self.message)