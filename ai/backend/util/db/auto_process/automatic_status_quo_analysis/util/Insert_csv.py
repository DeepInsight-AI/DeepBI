import os

import pandas as pd
from docx import Document
from docx.shared import Pt  # 设置字号
from docx.shared import Cm  # 设置宽度
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.export.export_path import get_export_path

document = Document()  # 创建表格实例

csv_path = os.path.join(get_export_path(), 'LAPASA_DE_advertising_data.csv')
df = pd.read_csv(csv_path, encoding='utf-8-sig')  # encoding 默认uft-8

row = df.shape[0] + 1  # 行数，加标题栏
col = df.shape[1]  # 列数

# 设置表格样式
table = document.add_table(rows=row, cols=col, style="Table Grid")

# 写入列标签
for i in range(1, col):
    table.cell(0, i).text = str(list(df.columns)[i])

# 针对行列索引为字符，表格数据为numpy格式
for i in range(1, row):
    for j in range(col):
        cell = table.cell(i, j)
        if str(type(df.iloc[i - 1, j])) == "<class 'str'>":
            table.cell(i - 1, j).width = Cm(4)  # 设置行索引格式
            cell.text = str(df.iloc[i - 1, j])  # 写入行索引

        if str(type(df.iloc[i - 1, j])) == "<class 'numpy.float64'>":
            cell.text = '%.2f' % (df.iloc[i - 1, j])  # 写入设置数值的保留小数

# table.autofit = True  # 表格自动适应窗口大小
# table.style.font.name = u'楷体'  # 设置字体格式
# table.style.font.size = Pt(12)  # 设置字体大小

document.save("test.docx")
