# filename: process_ad_campaigns.py
import pandas as pd

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# Filter for campaigns meeting the criteria
filtered_df = df[
    (df['ACOS_7d'] < 0.24) &
    (df['ACOS_yesterday'] < 0.24) &
    (df['cost_yesterday'] > 0.8 * df['Budget'])
]

# Update the budget for the filtered campaigns
filtered_df['New_Budget'] = filtered_df['Budget'] * 1.2
filtered_df['New_Budget'] = filtered_df['New_Budget'].apply(lambda x: x if x <= 50 else 50)

# Add a reason for the budget increase
filtered_df['Reason'] = 'ACOS_7d < 0.24, ACOS_yesterday < 0.24, cost_yesterday > 80% of Budget'

# Select the requested columns for the output
output_df = filtered_df[[
    'campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday', 
    'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 
    'total_clicks_30d', 'total_sales14d_30d', 'Reason'
]]

# Save the result to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_v1_1_IT_2024-06-17.csv'
output_df.to_csv(output_file_path, index=False)

print("Processing complete. The output has been saved to:", output_file_path)