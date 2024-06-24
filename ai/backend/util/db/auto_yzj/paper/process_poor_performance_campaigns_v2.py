# filename: process_poor_performance_campaigns_v2.py

import pandas as pd

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义今天的日期和昨天的日期
today = pd.Timestamp('2024-05-28')
yesterday = today - pd.Timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 基于定义一筛选广告活动
condition1 = (
    (data['avg_ACOS_7d'] > 0.24) &
    (data['ACOS'] > 0.24) &
    (data['clicks'] >= 10) &
    (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])
)

# 基于定义二筛选广告活动
condition2 = (
    (data['avg_ACOS_7d'] > 0.24) &
    (data['ACOS'] > 0.24) &
    (data['cost'] > 0.8 * data['Budget']) &
    (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])
)

# 基于定义三筛选广告活动
condition3 = (
    (data['avg_ACOS_1m'] > 0.24) &
    (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m']) &
    (data['clicks_7d'] >= 15) & 
    (data['sales_1m'] == 0)
)

# 满足条件的广告活动
poor_performance_campaigns = data[condition1 | condition2 | condition3].copy()

# 昨天的广告活动，且满足预算条件
def adjust_budget(row):
    if condition1[row.name]:
        new_budget = max(row['Budget'] - 5, 8)
        reason = '定义一'
    elif condition2[row.name]:
        new_budget = max(row['Budget'] - 5, 8)
        reason = '定义二'
    elif condition3[row.name]:
        new_budget = max(row['Budget'] - 5, 5)
        reason = '定义三'
    else:
        new_budget = row['Budget']
        reason = ''
    return pd.Series([new_budget, reason])

poor_performance_campaigns[['new_budget', 'reason']] = poor_performance_campaigns.apply(adjust_budget, axis=1)

# 选择需要的列并保存
columns_to_save = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 
    'clicks_7d', 'sales_1m', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_budget', 'reason'
]
result = poor_performance_campaigns[columns_to_save]

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_IT_2024-06-11.csv'
result.to_csv(output_path, index=False)

print("结果已保存到:", output_path)