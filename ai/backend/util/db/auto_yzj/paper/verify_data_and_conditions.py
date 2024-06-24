# filename: verify_data_and_conditions.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 打印数据的前几行
print("数据集的前几行:")
print(df.head())

# 打印数据集中各列的统计信息
print("\n数据集各列的统计信息:")
print(df.describe())

# 打印clicks_1m和sales_1m列的非零行数
print("\nclicks_1m和sales_1m列的非零行数:")
print(df[(df['clicks_1m'] > 0) & (df['sales_1m'] > 0)].shape[0])