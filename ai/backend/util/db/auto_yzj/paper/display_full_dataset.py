# filename: display_full_dataset.py

import pandas as pd

# 数据集路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 打印数据集的前几行
print("===== 数据集的前几行 =====")
print(df.head())

# 打印数据集的整体信息
print("\n===== 数据集的整体信息 =====")
print(df.info())

# 打印数据集的所有数据
print("\n===== 数据集的所有数据 =====")
print(df)