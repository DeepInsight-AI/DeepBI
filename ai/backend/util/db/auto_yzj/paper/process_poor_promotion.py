# filename: process_poor_promotion.py

import pandas as pd

# Define the filepath
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_UK_2024-07-09.csv'

# Read the CSV file
df = pd.read_csv(file_path)

# Define conditions based on the given definitions

# Initialize the new `New_keywordBid` column with current `keywordBid` values
df['New_keywordBid'] = df['keywordBid']

# Defining reasons for adjustments or closures
df['Action_reason'] = ""

# Definition 1
condition_1 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.5)
df.loc[condition_1, 'New_keywordBid'] = df['keywordBid'] / (((df['ACOS_7d'] - 0.24) / 0.24) + 1)
df.loc[condition_1, 'Action_reason'] = "Definition 1: Adjusted bid"

# Definition 2
condition_2 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] <= 0.36)
df.loc[condition_2, 'New_keywordBid'] = df['keywordBid'] / (((df['ACOS_7d'] - 0.24) / 0.24) + 1)
df.loc[condition_2, 'Action_reason'] += " Definition 2: Adjusted bid"

# Definition 3
condition_3 = (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] <= 0.36)
df.loc[condition_3, 'New_keywordBid'] = df['keywordBid'] - 0.04
df.loc[condition_3, 'Action_reason'] += " Definition 3: Reduced bid by 0.04"

# Definition 4
condition_4 = (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] > 0.5)
df.loc[condition_4, 'New_keywordBid'] = "关闭"
df.loc[condition_4, 'Action_reason'] += " Definition 4: Closed due to no sales and high ACOS"

# Definition 5
condition_5 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] > 0.36)
df.loc[condition_5, 'New_keywordBid'] = "关闭"
df.loc[condition_5, 'Action_reason'] += " Definition 5: Closed due to high ACOS both 7d and 30d"

# Definition 6
condition_6 = (df['total_sales14d_30d'] == 0) & (df['total_cost_30d'] >= 5)
df.loc[condition_6, 'New_keywordBid'] = "关闭"
df.loc[condition_6, 'Action_reason'] += " Definition 6: Closed due to no sales and high cost in 30d"

# Definition 7
condition_7 = (df['total_sales14d_30d'] == 0) & (df['total_clicks_30d'] >= 15) & (df['total_clicks_7d'] > 0)
df.loc[condition_7, 'New_keywordBid'] = "关闭"
df.loc[condition_7, 'Action_reason'] += " Definition 7: Closed due to no sales and sufficient clicks"

# Filter the DataFrame for the rows where actions are to be taken
filtered_df = df[df['Action_reason'] != ""]

# Selecting relevant columns for output
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid',
    'targeting', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_30d', 'ACOS_7d', 'ACOS_30d', 
    'total_clicks_30d', 'Action_reason'
]
output_df = filtered_df[output_columns]

# Save the results to a new CSV file
output_df.to_csv(output_file_path, index=False)

print(f"Processed data saved to {output_file_path}")