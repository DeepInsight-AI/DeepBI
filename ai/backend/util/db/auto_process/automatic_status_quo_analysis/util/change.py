import os
import win32com.client as win32
from docx import Document

def convert_to_pdf(input_path, output_path):
    # 创建Word应用程序实例
    word_app = win32.gencache.EnsureDispatch('Word.Application')
    # 设置应用程序可见性为False（不显示Word界面）
    word_app.Visible = False

    try:
        # 打开Word文档
        doc = word_app.Documents.Open(input_path)
        # 保存为PDF
        doc.SaveAs(output_path, FileFormat=17)
        doc.Close()
        return True
    except Exception as e:
        print("转换失败：" + str(e))
        return False
    finally:
        # 关闭Word应用程序
        word_app.Quit()



