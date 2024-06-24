# filename: budget_optimization_check_conditions.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 检查各条件单独满足的广告活动数量
condition1_part1 = df['avg_ACOS_7d'] > 0.24
condition1_part2 = df['ACOS'] > 0.24
condition1_part3 = df['clicks'] >= 10
condition1_part4 = df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']

condition2_part1 = df['avg_ACOS_7d'] > 0.24
condition2_part2 = df['ACOS'] > 0.24
condition2_part3 = df['cost'] > df['Budget'] * 0.8
condition2_part4 = df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']

condition3_part1 = df['avg_ACOS_1m'] > 0.24
condition3_part2 = df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']
condition3_part3 = df['sales_1m'] == 0
condition3_part4 = df['clicks_7d'] >= 15

# 输出每个部分满足的广告活动数量
print(f"定义一部分1满足条件广告活动数量: {df[condition1_part1].shape[0]}")
print(f"定义一部分2满足条件广告活动数量: {df[condition1_part2].shape[0]}")
print(f"定义一部分3满足条件广告活动数量: {df[condition1_part3].shape[0]}")
print(f"定义一部分4满足条件广告活动数量: {df[condition1_part4].shape[0]}")

print(f"定义二部分1满足条件广告活动数量: {df[condition2_part1].shape[0]}")
print(f"定义二部分2满足条件广告活动数量: {df[condition2_part2].shape[0]}")
print(f"定义二部分3满足条件广告活动数量: {df[condition2_part3].shape[0]}")
print(f"定义二部分4满足条件广告活动数量: {df[condition2_part4].shape[0]}")

print(f"定义三部分1满足条件广告活动数量: {df[condition3_part1].shape[0]}")
print(f"定义三部分2满足条件广告活动数量: {df[condition3_part2].shape[0]}")
print(f"定义三部分3满足条件广告活动数量: {df[condition3_part3].shape[0]}")
print(f"定义三部分4满足条件广告活动数量: {df[condition3_part4].shape[0]}")