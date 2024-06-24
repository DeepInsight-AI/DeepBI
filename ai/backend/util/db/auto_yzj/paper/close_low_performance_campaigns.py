# filename: close_low_performance_campaigns.py

import pandas as pd
from datetime import datetime, timedelta

# Step 1: Load Data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# Step 2: Identify Low Performance Campaigns Based on Definition
yesterday = datetime(2024, 5, 28) - timedelta(days=1)
low_performance_campaigns = df[(df['sales_1m'] == 0) & (df['clicks_1m'] >= 75)]

# Step 3: Add Closure Reason
low_performance_campaigns['closure_reason'] = 'Low performance: sales_1m = 0 and clicks_1m >= 75'

# Step 4: Select Required Columns
output_columns = [
    'date',
    'campaignName',
    'Budget',
    'clicks',
    'ACOS',
    'avg_ACOS_7d',
    'avg_ACOS_1m',
    'clicks_1m',
    'sales_1m',
    'closure_reason'
]

result_df = low_performance_campaigns[output_columns]

# Step 5: Output Results to CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_关闭的广告活动_ES_2024-06-07.csv'
result_df.to_csv(output_file_path, index=False)

print(f"Result saved to {output_file_path}")