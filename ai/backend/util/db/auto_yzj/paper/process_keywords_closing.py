# filename: process_keywords_closing.py

import pandas as pd
from datetime import datetime

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选满足定义一的关键词
condition_one = (
    (df['total_sales14d_7d'] == 0) &
    (df['total_clicks_7d'] > 0) &
    (df['total_sales14d_30d'] == 0) &
    (df['total_clicks_30d'] > 10)
)

# 筛选满足定义二的关键词
condition_two = (
    (df['total_sales14d_7d'] == 0) &
    (df['total_clicks_7d'] > 0) &
    (df['ACOS_30d'] > 0.5)
)

# 筛选满足定义三的关键词
condition_three = (
    (df['ACOS_7d'] > 0.5) &
    (df['ACOS_30d'] > 0.24)
)

# 综合所有条件
filtered_df = df[condition_one | condition_two | condition_three].copy()

# 添加提价的原因列
filtered_df['提价的原因'] = 'Condition Met'

# 选择需要输出的列
output_columns = [
    'campaignName',
    'adGroupName',
    'keyword',
    'ACOS_30d',
    'ACOS_7d',
    '提价的原因'
]
output_df = filtered_df[output_columns]

# 保存为新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_关闭自动定位组_ES_2024-06-07.csv'
output_df.to_csv(output_file_path, index=False)

print(f"结果已保存至：{output_file_path}")