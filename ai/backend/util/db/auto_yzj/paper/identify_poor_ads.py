# filename: identify_poor_ads.py

import pandas as pd
from datetime import datetime, timedelta

# 1. 数据加载
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 2. 数据分析
# 假设今天是2024年5月28日，所以昨天是 2024年5月27日
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)

# 筛选符合条件的广告活动
poor_performance_ads = df[(df['sales_1m'] == 0) & (df['clicks_1m'] >= 75)]

# 3. 输出结果
# 选择所需列
columns_to_output = [
    'campaignName', 'Budget', 'clicks', 'ACOS', 
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m'
]

# 加入关闭原因
poor_performance_ads['关闭原因'] = '最近一个月的总sales为0且点击次数大于等于75'

# 选择需要的列
result_df = poor_performance_ads[columns_to_output + ['关闭原因']]

# 输出到CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_关闭的广告活动_ES_2024-06-121.csv'
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"关闭的广告活动详情已保存到 {output_file_path}")