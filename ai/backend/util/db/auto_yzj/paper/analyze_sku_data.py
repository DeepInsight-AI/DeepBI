# filename: analyze_sku_data.py

import pandas as pd

# 数据集路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 显示基本统计信息
print("===== 基本统计信息 =====")
print(df.describe())

# 过滤条件具体统计信息
condition_1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
condition_2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['total_clicks_7d'] == 0)

print("\n===== 满足定义一的记录数 =====")
print(condition_1.sum())

print("\n===== 满足定义二的记录数 =====")
print(condition_2.sum())

print("\n===== 满足任一条件的记录数 =====")
print((condition_1 | condition_2).sum())