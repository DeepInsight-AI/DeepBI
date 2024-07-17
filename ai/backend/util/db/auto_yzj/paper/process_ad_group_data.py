# filename: process_ad_group_data.py

import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
df = pd.read_csv(file_path)

# Group by adGroupName and calculate the sum of total_sales_15d and total_clicks_7d within each group
grouped_df = df.groupby('adGroupName').agg({
    'total_sales_15d': 'sum',
    'total_clicks_7d': 'sum'
}).reset_index()

# Filter groups where total_sales_15d is 0 and total_clicks_7d of all items is less than or equal to 12
poor_performing_groups = grouped_df[(grouped_df['total_sales_15d'] == 0) & (grouped_df['total_clicks_7d'] <= 12)]['adGroupName']

# Filter the original dataframe to get entries of these poor performing groups
result_df = df[df['adGroupName'].isin(poor_performing_groups)].copy()

# Increase keyword bid by 0.02
result_df['new_keywordBid'] = result_df['keywordBid'] + 0.02

# Add operation reason
result_df['operation_reason'] = 'Low performance: 0 sales in 15 days, clicks in 7 days <= 12'

# Select and reorder columns for the final result
result_df = result_df[[
    'campaignName',
    'adGroupName',
    'total_sales_15d',
    'total_clicks_7d',
    'keyword',
    'matchType',
    'keywordBid',
    'keywordId',
    'new_keywordBid',
    'operation_reason'
]]

# Save the result to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_UK_2024-07-03.csv'
result_df.to_csv(output_file_path, index=False)

print(f'Results saved to {output_file_path}')