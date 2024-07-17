# filename: process_special_ads.py

import pandas as pd

# Read the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
df = pd.read_csv(file_path)

# Filter out ad groups with total_sales_15d == 0
ad_groups_with_zero_sales = df[df['total_sales_15d'] == 0]['adGroupName'].unique()

# Filter keywords in these ad groups with total_clicks_7d <= 12
filtered_keywords = df[(df['adGroupName'].isin(ad_groups_with_zero_sales)) & (df['total_clicks_7d'] <= 12)]

# Increase their bid by 0.02
filtered_keywords['new_keywordBid'] = filtered_keywords['keywordBid'] + 0.02
filtered_keywords['reason'] = 'Increase bid by 0.02 due to low sales and clicks'

# Select relevant columns
output_columns = ['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 'matchType', 'keywordBid', 'keywordId', 'new_keywordBid', 'reason']
output_df = filtered_keywords[output_columns]

# Output to CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_UK_2024-07-09.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Output saved to {output_file_path}")