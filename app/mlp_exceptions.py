
# 变量不存在异常类
class VariableNotFoundError(Exception):

    def __init__(self, variable_name):
        self.variable_name = variable_name
        super().__init__(
            f"未找到变量 '{variable_name}'。\n" + \
            f"Variable '{variable_name}' not found."
        )


# 数据不存在异常类
class DataNotFoundError(Exception):

    def __init__(self, data_name):
        self.data_name = data_name
        super().__init__(
            f"未找到数据 '{data_name}'。\n" + \
            f"Data '{data_name}' not found."
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


# 依赖缺失异常
class DependencyError(Exception):

    def __init__(self, dependency_name):
        self.dependency_name = dependency_name
        super().__init__(
            f"未找到依赖 '{dependency_name}'。\n" + \
            f"Dependency '{dependency_name}' not found."
        )


# 无文档异常
class DocuNotFoundError(Exception):

    def __init__(self):
        super().__init__(
            f"当前无文档。\n" + \
            f"Docu not Found."
        )


# 算子执行失败异常
class OperatorRunError(Exception):

    def __init__(self, object_name):
        super().__init__(
            f"算子 '{object_name}' 执行失败。\n" + \
            f"Failed to run operator '{object_name}'."
        )


# 用途不存在异常
class VarUsageNotFoundError(Exception):

    def __init__(self, usage_name):
        super().__init__(
            f"变量用途 '{usage_name}' 不存在。\n" + \
            f"VarUsage '{usage_name}' not found."
        )