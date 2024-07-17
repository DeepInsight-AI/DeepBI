# filename: identify_poor_ad_placements.py

import pandas as pd

# Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# Define conditionally flagged rows and their new bids or closure
def calculate_new_keyword_bid(row):
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        return max(row['keywordBid'] - 0.04, 0)
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        return '关闭'
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        return '关闭'
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        return '关闭'
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        return '关闭'
    else:
        return row['keywordBid']

# Apply the function to each row in the DataFrame to calculate the new bid or closure status
data['New_keywordBid'] = data.apply(calculate_new_keyword_bid, axis=1)

# Filter only the rows that have a New_keywordBid different from the original keywordBid (indicating they need action)
filtered_data = data.loc[data['New_keywordBid'] != data['keywordBid']]

# Specify the selected columns
required_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 
    'matchType', 'keywordBid', 'New_keywordBid', 'targeting', 
    'total_cost_7d', 'total_sales14d_7d', 'total_cost_7d', 
    'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', 'total_clicks_7d'
]

# Save the filtered data to a new CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_FR_2024-07-10.csv'
filtered_data.to_csv(output_path, index=False, columns=required_columns)

print(f"Filtered data has been saved to {output_path}")