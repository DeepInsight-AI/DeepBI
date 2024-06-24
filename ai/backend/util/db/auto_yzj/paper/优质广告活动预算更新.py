# filename: 优质广告活动预算更新.py

import pandas as pd
from datetime import datetime, timedelta

# 定义文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_ES_2024-06-07.csv'

# 读取数据
df = pd.read_csv(input_file)

# 确定分析日期（昨天的日期）
today = datetime.strptime("2024-05-28", "%Y-%m-%d")
yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")

# 筛选符合条件的广告活动
filtered_df = df[(df['date'] == yesterday) &
                 (df['avg_ACOS_7d'] < 0.24) &
                 (df['ACOS'] < 0.24) &
                 (df['cost'] > 0.8 * df['Budget'])]

# 增加预算的理由
filtered_df['increase_reason'] = "符合最近7天和昨天的ACOS值在0.24以下且昨天花费超过预算的80%"

# 增加预算
filtered_df['new_Budget'] = filtered_df['Budget'] * 1.20
filtered_df.loc[filtered_df['new_Budget'] > 50, 'new_Budget'] = 50

# 需要输出的结果
output_columns = [
    'date',
    'campaignName',
    'Budget',
    'cost',
    'clicks',
    'ACOS',
    'avg_ACOS_7d',
    'avg_ACOS_1m',
    'clicks_1m',
    'sales_1m',
    'increase_reason'
]
output_df = filtered_df[output_columns]

# 将结果写入CSV文件
output_df.to_csv(output_file, index=False)

print(f"结果已保存到 {output_file}")