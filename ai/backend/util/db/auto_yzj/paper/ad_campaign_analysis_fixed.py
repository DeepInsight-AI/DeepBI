# filename: ad_campaign_analysis_fixed.py

import pandas as pd
from datetime import timedelta

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\预处理.csv'
data = pd.read_csv(file_path)

# 假设今天是2024年5月28日
today = pd.Timestamp('2024-05-28')
yesterday = today - timedelta(days=1)

# 筛选出昨天的广告数据
yesterdays_data = data[data['date'] == yesterday.strftime('%Y-%m-%d')]

# 根据定义一的条件筛选劣质广告活动
filtered_data = yesterdays_data[
    (yesterdays_data['avg_ACOS_7d'] > 0.24) &
    (yesterdays_data['ACOS'] > 0.24) &
    (yesterdays_data['clicks'] >= 10) &
    (yesterdays_data['avg_ACOS_1m'] > yesterdays_data['country_avg_ACOS_1m'])
].copy()

# 对广告活动进行预算调整，并记录原因
def adjust_budget(row):
    new_budget = max(8, row['Budget'] - 5)
    reason = '低于标准：7天ACOS > 0.24, 昨天ACOS > 0.24, 昨天点击 >= 10, 月均ACOS > 国家均值ACOS'
    return pd.Series([new_budget, reason])

filtered_data[['new_Budget', 'reason']] = filtered_data.apply(adjust_budget, axis=1)

# 增加所需的字段信息
filtered_data = filtered_data[[
    'date', 
    'campaignName', 
    'Budget', 
    'clicks', 
    'ACOS', 
    'avg_ACOS_7d', 
    'clicks_7d', 
    'avg_ACOS_1m', 
    'clicks_1m', 
    'sales_1m',
    'country_avg_ACOS_1m', 
    'new_Budget', 
    'reason'
]]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\提问策略\测试_ES_2024-06-10.csv'
filtered_data.to_csv(output_file_path, index=False)

print("执行成功，结果已保存在指定CSV文件中。")