# filename: filter_ads_to_close.py

import pandas as pd
from datetime import datetime, timedelta

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义昨天的日期
yesterday = datetime(2024, 5, 28) - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 筛选出劣质广告活动
poor_ads = df[(df['sales_1m'] == 0) & (df['clicks_1m'] >= 75)]

# 增加关闭原因
poor_ads['关闭原因'] = '该广告活动最近一个月的总sales为0，且总点击次数大于等于75'

# 选择所需字段
result = poor_ads[['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', '关闭原因']]

# 确保日期字段格式正确
result['date'] = yesterday_str

# 输出到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_关闭的广告活动_IT_2024-06-11.csv'
result.to_csv(output_path, index=False)

print("广告活动关闭策略已保存至:", output_path)