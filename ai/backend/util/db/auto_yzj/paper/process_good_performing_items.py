# filename: process_good_performing_items.py

import pandas as pd

# Load the data from the provided file path
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# Define the conditions based on the definitions provided
condition1 = (
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & 
    (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) & 
    (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2)
)

condition2 = (
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & 
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & 
    (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2)
)

condition3 = (
    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & 
    (data['ACOS_30d'] <= 0.1) & 
    (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2)
)

condition4 = (
    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & 
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & 
    (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2)
)

condition5 = (
    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & 
    (data['ACOS_30d'] <= 0.1) & 
    (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2)
)

condition6 = (
    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & 
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & 
    (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2)
)

# Create a new DataFrame for results
columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 
    'matchType', 'keywordBid', 'New_keywordBid', 'targeting', 
    'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 
    'ACOS_30d', 'ORDER_1m', 'bid_increase', 'reason'
]
results = pd.DataFrame(columns=columns)

# Helper function to apply bid increase and reason
def apply_bid_increase(row, increase, reason):
    row['New_keywordBid'] = row['keywordBid'] + increase
    row['bid_increase'] = increase
    row['reason'] = reason
    return row

# Check conditions and apply increases
increases = [
    (condition1, 0.05, '定义一'),
    (condition2, 0.03, '定义二'),
    (condition3, 0.04, '定义三'),
    (condition4, 0.02, '定义四'),
    (condition5, 0.02, '定义五'),
    (condition6, 0.01, '定义六')
]

for condition, increase, reason in increases:
    subset = data[condition].copy()
    if not subset.empty:
        subset = subset.apply(apply_bid_increase, increase=increase, reason=reason, axis=1)
        results = pd.concat([results, subset[columns]])

# Save to CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_FR_2024-07-15.csv'
results.to_csv(output_file_path, index=False)

print(f"Results saved to {output_file_path}")