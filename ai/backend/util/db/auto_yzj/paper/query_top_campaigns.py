# filename: query_top_campaigns.py
import pandas as pd
from datetime import datetime, timedelta

# Define the file paths
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_IT_2024-06-05.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(input_file_path)

# Convert the 'date' column to datetime
df['date'] = pd.to_datetime(df['date'])

# Assume today is 2024-05-28 and calculate yesterday's date
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)

# Filter for rows corresponding to yesterday
df_yesterday = df[df['date'] == yesterday]

# Criteria for a well-performing campaign
is_good_campaign = (
    (df_yesterday['ACOS'] < 0.24) &
    (df_yesterday['cost'] > 0.8 * df_yesterday['Budget']) &
    (df_yesterday['avg_ACOS_7d'] < 0.24)
)

# Filter campaigns based on the criteria
good_campaigns = df_yesterday[is_good_campaign].copy()

# Increase the budget by 1/5, capping at 50
good_campaigns['New_Budget'] = good_campaigns['Budget'] * 1.2
good_campaigns['New_Budget'] = good_campaigns['New_Budget'].apply(lambda x: min(x, 50))

# Add reason for increasing budget
good_campaigns['Reason'] = 'Performance criteria met. Increased budget by 1/5.'

# Select relevant columns to output
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
    'Reason'
]
good_campaigns_output = good_campaigns[output_columns]

# Save the result to a new CSV file
good_campaigns_output.to_csv(output_file_path, index=False)

print(f"输出结果保存在: {output_file_path}")