# filename: update_ad_campaigns.py

import pandas as pd

# Step 1: Read the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# Step 2: Filter the campaigns based on the given criteria
filter_condition = (df['ACOS_7d'] < 0.24) & (df['ACOS_yesterday'] < 0.24) & (df['cost_yesterday'] > 0.8 * df['Budget'])
filtered_df = df[filter_condition].copy()

# Step 3: Update the budget
filtered_df['New_Budget'] = (filtered_df['Budget'] * 1.2).clip(upper=50)

# Step 4: Add the reason for the budget increase
filtered_df['Reason'] = '优质广告活动，昨天花费超过预算80%，预算增加1/5，直到总预算为50'

# Step 5: Select and arrange the columns to be outputted
output_columns = [
    'campaignId', 
    'campaignName', 
    'Budget',
    'New_Budget', 
    'cost_yesterday',
    'clicks_yesterday',
    'ACOS_yesterday', 
    'ACOS_7d',
    'total_clicks_30d', 
    'total_sales14d_30d',
    'Reason'
]

output_df = filtered_df[output_columns]

# Step 6: Save the result to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_v1_1_IT_2024-06-13.csv'
output_df.to_csv(output_file_path, index=False)

print("The filtered and updated ad campaigns have been successfully saved to the CSV file.")