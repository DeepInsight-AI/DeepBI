# filename: close_poor_performing_campaigns.py

import pandas as pd
from datetime import datetime, timedelta

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# Filter the data based on the conditions:
# - Total sales for the past month is 0
# - Total clicks for the past month are greater than or equal to 75
filtered_data = data[(data['sales_1m'] == 0) & (data['clicks_1m'] >= 75)]

# Extract relevant columns and add reason for closure
columns_needed = [
    "campaignName",
    "Budget",
    "clicks",
    "ACOS",
    "avg_ACOS_7d",
    "avg_ACOS_1m",
    "clicks_1m",
    "sales_1m"
]
filtered_data = filtered_data[columns_needed]
filtered_data['closure_reason'] = 'Total sales for the past month is 0 and total clicks for the past month are greater than or equal to 75'

# Save the output to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_关闭的广告活动_ES_2024-06-05.csv'
filtered_data.to_csv(output_file_path, index=False)

print(f"Filtered data has been saved to {output_file_path}")