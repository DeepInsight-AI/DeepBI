# filename: 提问策略_字段检查.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv"
data = pd.read_csv(file_path)

# 打印所有的列名
print("字段名列表:", data.columns.tolist())