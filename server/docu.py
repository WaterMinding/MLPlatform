# 导包
import os
import time
import base64
from io import BytesIO
from exception import ParamTypeException
from exception import PathNotExistsException
from exception import RedundantOperationException


# 类型检查函数
def type_check(var,var_name:str,correct_type:type):
    
    if type(var) != correct_type:

        raise ParamTypeException(str(var_name) + '应当为' + str(correct_type))


# 定义 “块” 类
class Cell:

    # 构造方法
    def __init__(self,cell_id:str,cell_type:str):

        # 检查参数类型
        type_check(cell_id,'cell_id',str)
        type_check(cell_type,'cell_type',str)
        
        # 定义块属性：块ID
        self.__id = cell_id

        # 定义块属性：块类型
        self.__type = cell_type


    # 返回块ID方法
    def get_cell_id(self) -> str:
        return self.__id


    # 返回块类型方法
    def get_cell_type(self) -> str:
        return self.__type


# 定义文本块类（继承Cell类）
class TextCell(Cell):

    # 构造方法
    def __init__(self,docu_name:str):

        # 检查参数类型
        type_check(docu_name,'docu_name',str)

        cell_id = docu_name + 'Text' + str(time.time())
        cell_type = 'Text'

        # 构造父类对象
        super().__init__(cell_id,cell_type)

        # 定义文本块属性：文本字符串
        self.__text = None
    

    # 更新文本字符串方法
    def set_text(self,new_text:str):

        # 检查参数类型
        type_check(new_text,'new_text',str)

        # 更新文本字符串
        self.__text = new_text
    

    # 返回文本字符串方法
    def get_text(self) -> str:

        return self.__text
    

    # 导出文本方法
    def export_txt(self,path:str):

        # 检查参数类型
        type_check(path,'path',str)

        # 检查与修饰路径
        if not os.path.exists(path):
            raise PathNotExistsException('导出路径不存在')
        path = path + '\\'

        # 创建、打开、编辑并保存txt文件
        with open(path + self.get_cell_id() + '.txt','w') as file:
            file.write(self.get_text())


# 定义图表块类
class ChartCell(Cell):

    # 构造方法
    def __init__(self, docu_name:str):

        # 检查参数类型
        type_check(docu_name,'docu_name',str)

        cell_id = docu_name + 'Chart' + str(time.time())
        cell_type = 'Chart'

        # 构造父类对象
        super().__init__(cell_id,cell_type)

        # 定义图表块属性：figure对象
        self.__figure = None
    

    # 注入figure对象方法
    def set_figure(self,plt_figure):

        # 检查是否已经存在figure对象
        if self.__figure == None:
            
            # 注入对象
            self.__figure = plt_figure

        else:
            raise RedundantOperationException('重复注入figure对象')

    
    # 返回figure对象方法
    def get_figure(self):

        return self.__figure
    

    # 返回图像编码
    def get_base64(self):

        # 图像保存至缓冲区
        buf = BytesIO()
        self.__figure.savefig(buf,format = 'png')
        buf.seek(0)

        # 图像base64编码
        image_base64 = base64.b64encode(buf.read()).decode('utf-8')

        # 包装为URL
        src = f"data:image/png;base64,{image_base64}"

        return src
    
    
    # 保存图像方法
    def save_figure(self,path:str):

        # 检查参数类型
        type_check(path,'path',str)

        # 检查与修饰路径
        if not os.path.exists(path):
            raise PathNotExistsException('导出路径不存在')
        path = path + '\\'

        # 保存图像
        self.__figure.savefig(path + self.get_cell_id() + '.png')

