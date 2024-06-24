# filename: process_ad_campaign.py
import pandas as pd
from datetime import datetime, timedelta

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# Define the conditions
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# Filtering based on the conditions
filtered_data = data[
    (data['avg_ACOS_7d'] < 0.24) &
    (data['ACOS'] < 0.24) &
    (data['cost'] > data['Budget'] * 0.8)
].copy()

# Adjusting Budget
filtered_data['new_Budget'] = filtered_data['Budget'] * 1.2
filtered_data['new_Budget'] = filtered_data['new_Budget'].apply(lambda x: min(x, 50))

# Creating the output DataFrame
output_columns = ['date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS',
                  'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m']
filtered_data = filtered_data.assign(date=yesterday_str)

filtered_data = filtered_data[output_columns]
filtered_data['Reason'] = "ACOS<0.24 for 7d avg and yesterday, cost>80% of budget"

# Save to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_ES_2024-06-10.csv'
filtered_data.to_csv(output_path, index=False)

print(f"Filtered data has been saved to {output_path}")