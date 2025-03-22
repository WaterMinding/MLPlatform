
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

# 算子不存在异常类
class OperatorNotFoundError(Exception):

    def __init__(self, operator_name):
        self.operator_name = operator_name
        super().__init__(
            f"未找到算子 '{operator_name}'。\n" + \
            f"Operator '{operator_name}' not found."
        )


# 绘图不存在异常类
class PlotterNotFoundError(Exception):

    def __init__(self, plotter_name):
        self.plotter_name = plotter_name
        super().__init__(
            f"未找到绘图 '{plotter_name}'。\n" + \
            f"Plotter '{plotter_name}' not found."
        )