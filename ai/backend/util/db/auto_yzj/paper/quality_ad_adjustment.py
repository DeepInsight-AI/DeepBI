# filename: quality_ad_adjustment.py

import pandas as pd

# Load the dataset
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(file_path)

# Filtering and calculation based on the definitions
df_filtered = df.copy()

# Define conditions based on the provided rules
conditions = [
    (df_filtered['ACOS_7d'] > 0.24) & (df_filtered['ACOS_7d'] <= 0.5) & (df_filtered['ACOS_30d'] > 0) & (df_filtered['ACOS_30d'] <= 0.5),
    (df_filtered['ACOS_7d'] > 0.5) & (df_filtered['ACOS_30d'] <= 0.36),
    (df_filtered['total_clicks_7d'] >= 10) & (df_filtered['total_sales14d_7d'] == 0) & (df_filtered['ACOS_30d'] <= 0.36),
    (df_filtered['total_clicks_7d'] > 10) & (df_filtered['total_sales14d_7d'] == 0) & (df_filtered['ACOS_30d'] > 0.5),
    (df_filtered['ACOS_7d'] > 0.5) & (df_filtered['ACOS_30d'] > 0.36),
    (df_filtered['total_sales14d_30d'] == 0) & (df_filtered['total_cost_30d'] >= 5),
    (df_filtered['total_sales14d_30d'] == 0) & (df_filtered['total_clicks_30d'] >= 15) & (df_filtered['total_clicks_7d'] > 0)
]

# Apply the rules
df_filtered['New_keywordBid'] = df_filtered['keywordBid']

for i, condition in enumerate(conditions):
    if i == 0 or i == 1:
        df_filtered.loc[condition, 'New_keywordBid'] = df_filtered['keywordBid'] / ((df_filtered['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif i == 2:
        df_filtered.loc[condition, 'New_keywordBid'] = df_filtered['keywordBid'] - 0.04
    else:
        df_filtered.loc[condition, 'New_keywordBid'] = '关闭'

# Keep only required columns
result_columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 
                  'targeting', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_yesterday', 'ACOS_7d', 'ACOS_30d', 
                  'total_clicks_30d']

df_result = df_filtered.loc[sum(conditions), result_columns]

# Save the result to a new CSV file
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_FR_2024-06-30.csv"
df_result.to_csv(output_path, index=False)

print(f"Filtered data saved to {output_path}")