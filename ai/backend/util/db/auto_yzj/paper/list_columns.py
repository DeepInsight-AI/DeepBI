# filename: list_columns.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv'
df = pd.read_csv(file_path)

# 输出数据集的列名
print(df.columns.tolist())