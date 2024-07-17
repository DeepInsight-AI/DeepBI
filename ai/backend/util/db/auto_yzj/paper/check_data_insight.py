# filename: check_data_insight.py

import pandas as pd

# 定义csv文件路径和输出文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'

# 读取csv文件
df = pd.read_csv(input_file)

# 查看数据的前几行
print("数据前几行：")
print(df.head())

# 查看各列的统计信息
print("\n数据说明：")
print(df.describe())

# 查看各列的数据类型
print("\n数据类型：")
print(df.dtypes)