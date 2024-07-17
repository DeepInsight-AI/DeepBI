# filename: check_data.py

import pandas as pd

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'

# 读取数据
df = pd.read_csv(input_file)

# 打印数据头部及统计信息
print("数据头部：")
print(df.head())
print("\n数据描述：")
print(df.describe())
print("\n列信息：")
print(df.info())