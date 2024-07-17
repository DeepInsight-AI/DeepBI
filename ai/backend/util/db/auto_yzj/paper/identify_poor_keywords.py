# filename: identify_poor_keywords.py

import pandas as pd

# Load the CSV data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\预处理.csv'
data = pd.read_csv(file_path)

# Ensure cost columns are not empty to avoid division errors later
data.fillna(0, inplace=True)

# Define the new_bid calculation based on provided rules
def calculate_new_bid(row):
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    bid = row['keywordBid']
    clicks_7d = row['total_clicks_7d']
    sales_7d = row['total_sales14d_7d']
    orders_1m = row['ORDER_1m']
    clicks_30d = row['total_clicks_30d']
    cost_30d = row['total_cost_30d']
    sales_30d = row['total_sales14d_30d']
    
    new_bid = bid # default same value
    reason = ''
    
    if 0.27 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and orders_1m < 5:
        new_bid = bid / ((avg_ACOS_7d - 0.27) / 0.27 + 1)
        reason = 'Rule 1 apply'
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36:
        new_bid = bid / ((avg_ACOS_7d - 0.27) / 0.27 + 1)
        reason = 'Rule 2 apply'
    elif clicks_7d >= 10 and sales_7d == 0 and avg_ACOS_30d <= 0.36:
        new_bid = bid - 0.04
        reason = 'Rule 3 apply'
    elif clicks_7d >= 10 and sales_7d == 0 and avg_ACOS_30d > 0.5:
        new_bid = '关闭'
        reason = 'Rule 4 apply'
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d > 0.36:
        new_bid = '关闭'
        reason = 'Rule 5 apply'
    elif sales_30d == 0 and cost_30d >= 5:
        new_bid = '关闭'
        reason = 'Rule 6 apply'
    elif sales_30d == 0 and clicks_30d >= 15 and clicks_7d > 0:
        new_bid = '关闭'
        reason = 'Rule 7 apply'
    
    return new_bid, reason

# Apply calculation to each row and generate a new DataFrame
result = []
for _, row in data.iterrows():
    new_bid, reason = calculate_new_bid(row)
    if reason: # Only include rows with reasons
        result.append({
            'keyword': row['keyword'],
            'keywordId': row['keywordId'],
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'matchType': row['matchType'],
            'keywordBid': row['keywordBid'],
            'new_keywordBid': new_bid,
            'targeting': row['targeting'],
            'cost': row['total_cost_30d'],
            'clicks': row['total_clicks_30d'],
            'total_cost_7d': row['total_cost_7d'],
            'total_sales14d_7d': row['total_sales14d_7d'],
            'avg_ACOS_7d': row['ACOS_7d'],
            'avg_ACOS_30d': row['ACOS_30d'],
            'total_clicks_30d': row['total_clicks_30d'],
            'reason': reason
        })

# Convert to DataFrame
final_df = pd.DataFrame(result)

# Save to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\提问策略\手动_劣质关键词_v1_1_LAPASA_IT_2024-07-03.csv'
final_df.to_csv(output_file_path, index=False)

print(f"Result saved to {output_file_path}")