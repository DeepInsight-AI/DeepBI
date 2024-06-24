# filename: process_budget_increase.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# Assume today's date is 2024-05-28 and calculate yesterday's date
today_date = datetime.strptime("2024-05-28", "%Y-%m-%d")
yesterday_date = today_date - timedelta(days=1)
yesterday_date_str = yesterday_date.strftime("%Y-%m-%d")

# Filter the data for yesterday
yesterday_data = df[df['date'] == yesterday_date_str]

# Conditions
condition_1 = yesterday_data['avg_ACOS_7d'] < 0.24
condition_2 = yesterday_data['ACOS'] < 0.24
condition_3 = yesterday_data['cost'] > (0.8 * yesterday_data['Budget'])

# Select campaigns that meet all the conditions
good_campaigns = yesterday_data[condition_1 & condition_2 & condition_3]

# Update the Budget
good_campaigns['Budget'] = np.minimum(good_campaigns['Budget'] * 1.2, 50)

# Add reason for budget increase
good_campaigns['reason_for_increase'] = "Recent 7-day avg ACOS < 0.24, Yesterday's ACOS < 0.24, Yesterday's cost > 80% of budget"

# Select necessary columns
output_data = good_campaigns[[
    'date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS', 'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'reason_for_increase'
]]

# Output the result to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_ES_2024-06-05.csv'
output_data.to_csv(output_file_path, index=False)

print(f"Output saved to {output_file_path}")