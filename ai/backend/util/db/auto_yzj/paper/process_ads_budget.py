# filename: process_ads_budget.py

import pandas as pd
from datetime import datetime, timedelta

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\提问策略\测试_IT_2024-06-11.csv'

# 读取 CSV 文件
df = pd.read_csv(input_file)

# 假设今天是2024年5月28日
today = datetime.strptime('2024-05-28', '%Y-%m-%d')
yesterday = today - timedelta(days=1)

# 筛选之前先处理日期类型字段
df['date'] = pd.to_datetime(df['date'])

# 定义一的条件
conditions = (
    (df['avg_ACOS_7d'] > 0.24) &
    (df['ACOS'] > 0.24) &
    (df['clicks'] >= 10) &
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']) &
    (df['date'] == yesterday)
)

# 过滤出符合条件的广告活动
filtered_df = df[conditions]

# 对广告活动预算降低5，直到预算为8
def adjust_budget(budget):
    new_budget = budget - 5
    if new_budget < 8:
        return 8
    return new_budget

filtered_df['new_budget'] = filtered_df['Budget'].apply(adjust_budget)

# 增加原因列
filtered_df['reason'] = 'Performance below defined thresholds; Budget reduced by 5 until it reaches 8'

# 选择输出所需的列
output_columns = ['date', 'campaignName', 'Budget', 'clicks', 'ACOS',
                  'avg_ACOS_7d', 'clicks_7d', 'sales_1m', 'avg_ACOS_1m',
                  'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_budget', 'reason']

output_df = filtered_df[output_columns]

# 保存到新的 CSV 文件
output_df.to_csv(output_file, index=False)

print("Script executed successfully and data is saved to", output_file)