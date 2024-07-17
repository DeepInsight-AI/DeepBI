# filename: search_term_analysis_v2.py

import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# Define conditions
conditions = [
    {
        'reason': '定义一',
        'condition': (df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5)
    },
    {
        'reason': '定义二',
        'condition': (df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8)
    },
    {
        'reason': '定义三',
        'condition': (df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0)
    },
    {
        'reason': '定义四',
        'condition': (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3)
    },
    {
        'reason': '定义五',
        'condition': (df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5)
    },
    {
        'reason': '定义六',
        'condition': (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0)
    }
]

# Filter data based on conditions
filtered_data = pd.DataFrame()
for cond in conditions:
    condition_met = df[cond['condition']]
    condition_met = condition_met.assign(reason=cond['reason'])
    filtered_data = pd.concat([filtered_data, condition_met])

# Ensure no duplicates
filtered_data.drop_duplicates(inplace=True)

# Select and rename columns as per the requirement
output_columns = [
    'campaignName',    # 广告活动
    'adGroupName',     # 广告组
    'total_clicks_7d', # 近七天的点击次数
    'ACOS_7d',         # 近七天的acos值
    'ORDER_7d',        # 近七天的订单数
    'total_clicks_30d',# 近一个月的总点击数
    'ORDER_1m',        # 近一个月的订单数
    'ACOS_30d',        # 近一个月的acos值
    'searchTerm',      # 搜索词
    'reason'           # 满足的定义
]

# Select needed columns
filtered_data = filtered_data[output_columns]

# Add Chinese column names for output
filtered_data.columns = [
    '广告活动',
    '广告组',
    '近七天的点击次数',
    '近七天的acos值',
    '近七天的订单数',
    '近一个月的总点击数',
    '近一个月的订单数',
    '近一个月的acos值',
    '搜索词',
    '满足的定义'
]

# Save the result to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_FR_2024-07-02.csv'
filtered_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"Filtered data has been saved to {output_file_path}")