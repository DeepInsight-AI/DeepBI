# filename: budget_optimization_check.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 计算描述性统计信息
print("描述性统计信息：\n", df.describe(include='all'))

# 假设今天是2024-05-28，找到昨天的日期
yesterday = "2024-05-27"

# 过滤出昨天的数据
yesterday_data = df[df['date'] == yesterday]

# 满足定义一的广告活动
condition1 = (
    (df['avg_ACOS_7d'] > 0.24) &
    (df['ACOS'] > 0.24) &
    (df['clicks'] >= 10) &
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
)

# 满足定义二的广告活动
condition2 = (
    (df['avg_ACOS_7d'] > 0.24) &
    (df['ACOS'] > 0.24) &
    (df['cost'] > df['Budget'] * 0.8) &
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
)

# 满足定义三的广告活动
condition3 = (
    (df['avg_ACOS_1m'] > 0.24) &
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']) &
    (df['sales_1m'] == 0) &
    (df['clicks_7d'] >= 15)
)

# 检查每个条件过滤后的记录数
num_condition1 = df[condition1].shape[0]
num_condition2 = df[condition2].shape[0]
num_condition3 = df[condition3].shape[0]

print(f"满足定义一的广告活动数量: {num_condition1}")
print(f"满足定义二的广告活动数量: {num_condition2}")
print(f"满足定义三的广告活动数量: {num_condition3}")