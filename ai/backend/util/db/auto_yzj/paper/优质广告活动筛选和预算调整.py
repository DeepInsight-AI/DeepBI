# filename: 优质广告活动筛选和预算调整.py

import pandas as pd
from datetime import datetime, timedelta

# 加载数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 假设今天是2024年5月28日，我们需要昨天的数据
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 筛选符合条件的广告活动
condition = (
    (df['date'] == yesterday_str) &  # 昨天的数据
    (df['avg_ACOS_7d'] < 0.24) &  # 最近7天的平均ACOS值在0.24以下
    (df['ACOS'] < 0.24) &  # 昨天的ACOS值在0.24以下
    (df['cost'] > (df['Budget'] * 0.8))  # 昨天花费超过了昨天预算的80%
)

df_filtered = df[condition].copy()

# 增加预算
def increase_budget(budget):
    new_budget = budget * 1.2  # 增加原来预算的1/5
    if new_budget > 50:
        new_budget = 50  # 直到预算为50
    return new_budget

df_filtered['New_Budget'] = df_filtered['Budget'].apply(increase_budget)

# 添加原因列
df_filtered['原因'] = '最近7天的平均ACOS值小于0.24，昨天的ACOS值小于0.24，昨天花费超过了预算的80%'

# 保存结果文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_ES_2024-06-05.csv'
df_filtered.to_csv(output_path, index=False, columns=[
    'date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS', 'avg_ACOS_7d', 'avg_ACOS_1m', 
    'clicks_1m', 'sales_1m', '原因', 'New_Budget'
])

print(f"结果已保存到 {output_path}")