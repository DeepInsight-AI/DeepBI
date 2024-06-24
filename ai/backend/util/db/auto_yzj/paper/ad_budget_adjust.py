# filename: ad_budget_adjust.py

import pandas as pd
from datetime import datetime, timedelta

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 将日期转换为datetime类型
df['date'] = pd.to_datetime(df['date'])

# 假设今天是2024年5月28日
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)

# 过滤出昨天的数据
yesterday_data = df[df['date'] == yesterday]

# 定义一条件
condition_1 = (
    (df['avg_ACOS_7d'] > 0.24) & 
    (yesterday_data['ACOS'] > 0.24) & 
    (yesterday_data['clicks'] >= 10) & 
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
)

# 定义二条件
condition_2 = (
    (df['avg_ACOS_7d'] > 0.24) & 
    (yesterday_data['ACOS'] > 0.24) & 
    (yesterday_data['cost'] > 0.8 * yesterday_data['Budget']) & 
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
)

# 定义三条件
condition_3 = (
    (df['avg_ACOS_1m'] > 0.24) & 
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']) & 
    (df['sales_1m'] == 0) & 
    (df['clicks_7d'] >= 15)
)

# 符合条件的广告活动
filtered_1 = yesterday_data[condition_1].copy()
filtered_2 = yesterday_data[condition_2].copy()
filtered_3 = yesterday_data[condition_3].copy()

# 更新预算
def update_budget(row, min_budget):
    new_budget = max(row['Budget'] - 5, min_budget)
    return new_budget

filtered_1['new_Budget'] = filtered_1.apply(update_budget, axis=1, min_budget=8)
filtered_2['new_Budget'] = filtered_2.apply(update_budget, axis=1, min_budget=8)
filtered_3['new_Budget'] = filtered_3.apply(update_budget, axis=1, min_budget=5)

# 添加劣质广告活动原因
filtered_1['reason'] = '定义一'
filtered_2['reason'] = '定义二'
filtered_3['reason'] = '定义三'

# 合并结果
result = pd.concat([filtered_1, filtered_2, filtered_3])

# 输出所需字段
output_columns = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 
    'avg_ACOS_7d', 'clicks_7d', 'sales_1m', 'avg_ACOS_1m', 
    'clicks_1m', 'sales_1m',  'country_avg_ACOS_1m', 'new_Budget', 'reason'
]
result = result[output_columns]

# 保存结果
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\劣质广告活动_FR.csv'
result.to_csv(output_path, index=False)

print(f"Results have been saved to {output_path}")