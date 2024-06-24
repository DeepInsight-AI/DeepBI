# filename: analyze_campaigns.py

import pandas as pd
from datetime import datetime, timedelta

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# Define today's date and calculate yesterday's date
today_date = datetime(2024, 5, 28)
yesterday_date = today_date - timedelta(days=1)
yesterday_str = yesterday_date.strftime('%Y-%m-%d')

# Filter data for poor performing campaigns
poor_performing = data[(data['sales_1m'] == 0) & (data['clicks_1m'] >= 75)]

# Add a closed reason column
poor_performing['关闭原因'] = '最近一个月的总sales为0且总点击次数大于等于75'

# Select relevant columns for the final output
output_columns = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', '关闭原因'
]
poor_performing = poor_performing[output_columns]

# Save the result to a new CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\关闭的广告活动_FR_2024-5-28.csv'
poor_performing.to_csv(output_path, index=False)

print(f"Filtered data saved to {output_path}")