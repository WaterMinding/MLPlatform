
# 变量不存在异常类
class VariableNotFoundError(Exception):

    def __init__(self, variable_name):
        self.variable_name = variable_name
        super().__init__(
            f"未找到变量 '{variable_name}'。\n" + \
            f"Variable '{variable_name}' not found."
        )


# 构造失败异常类
class ConstructionError(Exception):

    def __init__(self, object_name):
        self.object_name = object_name
        super().__init__(
            f"构造 '{object_name}' 失败。\n" + \
            f"Failed to construct '{object_name}'."
        ) 
