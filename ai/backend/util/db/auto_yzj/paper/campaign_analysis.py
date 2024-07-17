# filename: campaign_analysis.py

import pandas as pd

# File path
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_DE_2024-06-28.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(input_file_path)

# Filter ad groups with total_sales_15d == 0
ad_groups_with_zero_sales = df[df['total_sales_15d'] == 0]['adGroupName'].unique()

# Filter and adjust the bids for keywords in these ad groups with total_clicks_7d <= 12
result_df = df[(df['adGroupName'].isin(ad_groups_with_zero_sales)) & (df['total_clicks_7d'] <= 12)].copy()
result_df['new_bid'] = result_df['keywordBid'] + 0.02
result_df['reason'] = 'Ad group total sales in last 15 days is zero and total clicks in last 7 days are less than or equal to 12'

# Select specific columns to save
output_columns = [
    'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword',
    'matchType', 'keywordBid', 'keywordId', 'new_bid', 'reason'
]

# Output the results to a new CSV file
result_df.to_csv(output_file_path, columns=output_columns, index=False)

print(f"符合条件的商品投放已输出到: {output_file_path}")