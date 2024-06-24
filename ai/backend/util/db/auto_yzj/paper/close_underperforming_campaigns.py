# filename: close_underperforming_campaigns.py

import pandas as pd
from datetime import datetime, timedelta

# Constants
INPUT_FILE_PATH = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv"
OUTPUT_FILE_PATH = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_关闭的广告活动_ES_2024-06-10.csv"
YESTERDAY = (datetime(2024, 5, 28) - timedelta(days=1)).strftime('%Y-%m-%d')

# Load the data
df = pd.read_csv(INPUT_FILE_PATH)

# Filter underperforming campaigns
underperforming_campaigns = df[
    (df['sales_1m'] == 0) &
    (df['clicks_1m'] >= 75)
]

# Prepare the output DataFrame
output_df = underperforming_campaigns[[
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS',
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m'
]].copy()

# Add reason column
output_df['reason'] = '最近一个月的总sales为0且总点击次数大于等于75'

# Save to CSV
output_df.to_csv(OUTPUT_FILE_PATH, index=False, encoding='utf-8-sig')

print(f"Underperforming campaigns have been saved to {OUTPUT_FILE_PATH}")