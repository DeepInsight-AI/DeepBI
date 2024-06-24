# filename: poor_performing_keywords.py

import pandas as pd
from datetime import datetime, timedelta

# Load the CSV file into a DataFrame
file_path = r"C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv"
df = pd.read_csv(file_path)

# Ensuring that necessary columns for the example below exist
# Add any additional necessary columns for the example below
# For example, keywordId, keyword, keywordBid, adGroupName, matchingType, targeting
required_columns = [
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_7d', 'sales_1m', 'clicks_1m', 
    'keywordId', 'keyword', 'keywordBid', 'adGroupName', 'matchingType', 
    'targeting', 'cost', 'clicks', 'cost_1m', 'cost_7d', 'ad_group_total_cost_7d'
]

for col in required_columns:
    if col not in df.columns:
        df[col] = 0  # Add column with default value 0 if it doesn't exist

# Helper function to determine new keyword bid
def calculate_new_bid(avg_ACOS_7d, keywordBid):
    return keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)

# Add today's date as a constant for analysis
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)

# Add columns for calculations; these columns ideally should be part of the CSV file
df['date'] = yesterday.strftime("%Y-%m-%d")  # Add yesterday's date for simplicity

# Initialize a list to store poor-performing keywords
poor_keywords = []

# Iterate through the DataFrame and apply the rules
for index, row in df.iterrows():
    keyword_info = {}
    met_criteria = False

    keywordBid = row.get('keywordBid', 0)

    # Definition 1
    if 0.24 < row['avg_ACOS_7d'] <= 0.5 and 0 < row['avg_ACOS_1m'] <= 0.5:
        keyword_info['new_keywordBid'] = calculate_new_bid(row['avg_ACOS_7d'], keywordBid)
        keyword_info['reason'] = "ACOS_7d > 0.24 and <= 0.5 and ACOS_1m > 0 and <= 0.5"
        met_criteria = True

    # Definition 2
    if row['avg_ACOS_7d'] > 0.5 and row['avg_ACOS_1m'] <= 0.36:
        keyword_info['new_keywordBid'] = calculate_new_bid(row['avg_ACOS_7d'], keywordBid)
        keyword_info['reason'] = "ACOS_7d > 0.5 and ACOS_1m <= 0.36"
        met_criteria = True

    # Definition 3
    if row['clicks_7d'] >= 10 and row.get('sales_7d', 0) == 0 and row['avg_ACOS_1m'] <= 0.36:
        keyword_info['new_keywordBid'] = keywordBid - 0.04
        keyword_info['reason'] = "clicks_7d >= 10 and sales_7d = 0 and ACOS_1m <= 0.36"
        met_criteria = True

    # Definition 4
    if row['clicks_7d'] >= 10 and row.get('sales_7d', 0) == 0 and row['avg_ACOS_1m'] > 0.5:
        keyword_info['new_keywordBid'] = 0
        keyword_info['reason'] = "clicks_7d >= 10 and sales_7d = 0 and ACOS_1m > 0.5"
        met_criteria = True

    # Definition 5
    if row['avg_ACOS_7d'] > 0.5 and row['avg_ACOS_1m'] > 0.36:
        keyword_info['new_keywordBid'] = 0
        keyword_info['reason'] = "ACOS_7d > 0.5 and ACOS_1m > 0.36"
        met_criteria = True

    # Definition 6
    total_cost_7d = row['cost_7d']
    ad_group_total_cost_7d = row['ad_group_total_cost_7d']

    if row['sales_1m'] == 0 and total_cost_7d > (ad_group_total_cost_7d / 5):
        keyword_info['new_keywordBid'] = 0
        keyword_info['reason'] = "sales_1m = 0 and cost_7d > (ad_group_total_cost_7d / 5)"
        met_criteria = True

    # Definition 7
    if row['sales_1m'] == 0 and row['clicks_1m'] >= 15:
        keyword_info['new_keywordBid'] = 0
        keyword_info['reason'] = "sales_1m = 0 and clicks_1m >= 15"
        met_criteria = True

    if met_criteria:
        keyword_info.update({
            'date': row['date'],
            'keyword': row['keyword'],
            'keywordId': row['keywordId'],
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'matchingType': row['matchingType'],
            'keywordBid': keywordBid,
            'targeting': row.get('targeting', ''),
            'cost': row.get('cost', 0),
            'clicks': row.get('clicks', 0),
            'cost_7d': total_cost_7d,
            'sales_7d': row.get('sales_7d', 0),
            'ad_group_total_cost_7d': ad_group_total_cost_7d,
            'avg_ACOS_7d': row['avg_ACOS_7d'],
            'avg_ACOS_1m': row['avg_ACOS_1m']
        })
        poor_keywords.append(keyword_info)

# Convert the poor keywords list to a DataFrame
poor_keywords_df = pd.DataFrame(poor_keywords)

# Save the poor-performing keywords to a CSV file
output_file_path = r"C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\劣质关键词.csv"
poor_keywords_df.to_csv(output_file_path, index=False)

print(f"Poor-performing keywords have been saved to {output_file_path}")