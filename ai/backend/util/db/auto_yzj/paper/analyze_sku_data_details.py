# filename: analyze_sku_data_details.py

import pandas as pd

# 数据集路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 查看ACOS相关字段的分布
print("===== ACOS 相关字段的分布情况 =====")
print(df[['ACOS_30d', 'ACOS_7d', 'ACOS_yesterday']].describe())

# 查看点击数相关字段的分布
print("\n===== 点击数相关字段的分布情况 =====")
print(df[['total_clicks_30d', 'total_clicks_7d', 'total_clicks_yesterday']].describe())

# 查看总销售额字段的分布
print("\n===== 销售额相关字段的分布情况 =====")
print(df[['total_sales14d_30d', 'total_sales14d_7d', 'total_sales14d_yesterday']].describe())