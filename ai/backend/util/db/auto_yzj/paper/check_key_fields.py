# filename: check_key_fields.py

import pandas as pd

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'

# 读取数据
df = pd.read_csv(input_file)

# 打印感兴趣字段的基本信息
print("30天的总订单数描述：")
print(df['ORDER_1m'].describe())

print("\n7天的总花费描述：")
print(df['total_cost_7d'].describe())

print("\n30天的平均ACOS值描述：")
print(df['ACOS_30d'].describe())

print("\n7天的销售额描述：")
print(df['total_sales_7d'].describe())