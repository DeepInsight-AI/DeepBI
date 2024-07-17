# filename: check_data_non_zero.py

import pandas as pd

# 定义csv文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'

# 读取csv文件
df = pd.read_csv(input_file)

# 过滤出ORDER_1m和ORDER_7d中至少一个不为零的数据行
non_zero_orders = df[(df['ORDER_1m'] > 0) | (df['ORDER_7d'] > 0)]

print("ORDER_1m和/或ORDER_7d中至少一个不为零的数据行：")
print(non_zero_orders[['ACOS_30d', 'ORDER_1m', 'ACOS_7d', 'ORDER_7d']].head(50))  # 输出前50行以便检查