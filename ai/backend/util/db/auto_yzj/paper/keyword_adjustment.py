# filename: keyword_adjustment.py

import pandas as pd

# Load the data from the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# Initialize lists to store results
results = []

# Define the conditions and corresponding bid adjustments or closure
for index, row in data.iterrows():
    keyword_id = row['keywordId']
    keyword = row['keyword']
    ad_group_name = row['adGroupName']
    campaign_name = row['campaignName']
    keyword_bid = row['keywordBid']
    acos_30d = row['ACOS_30d']
    acos_7d = row['ACOS_7d']
    clicks_7d = row['total_clicks_7d']
    sales_7d = row['total_sales14d_7d']
    sales_30d = row['total_sales14d_30d']
    clicks_30d = row['total_clicks_30d']

    new_bid = keyword_bid
    action_desc = ""

    if 0.24 < acos_7d < 0.5 and 0 < acos_30d < 0.24:
        new_bid -= 0.03
        action_desc = "Lower bid by 0.03 (Definition 1)"
    elif 0.24 < acos_7d < 0.5 and 0.24 < acos_30d < 0.5:
        new_bid -= 0.04
        action_desc = "Lower bid by 0.04 (Definition 2)"
    elif sales_7d == 0 and clicks_7d > 0 and 0.24 < acos_30d < 0.5:
        new_bid -= 0.04
        action_desc = "Lower bid by 0.04 (Definition 3)"
    elif 0.24 < acos_7d < 0.5 and acos_30d > 0.5:
        new_bid -= 0.05
        action_desc = "Lower bid by 0.05 (Definition 4)"
    elif acos_7d > 0.5 and 0 < acos_30d < 0.24:
        new_bid -= 0.05
        action_desc = "Lower bid by 0.05 (Definition 5)"
    elif sales_30d == 0 and clicks_30d > 13 and clicks_7d > 0:
        new_bid = "关闭"
        action_desc = "Close the keyword (Definition 6)"
    elif sales_7d == 0 and clicks_7d > 0 and acos_30d > 0.5:
        new_bid = "关闭"
        action_desc = "Close the keyword (Definition 7)"
    elif acos_7d > 0.5 and acos_30d > 0.24:
        new_bid = "关闭"
        action_desc = "Close the keyword (Definition 8)"

    if action_desc:
        results.append([
            campaign_name, 
            ad_group_name, 
            keyword, 
            keyword_bid, 
            new_bid, 
            acos_30d, 
            acos_7d, 
            clicks_7d, 
            action_desc
        ])

# Create a DataFrame to hold the results
columns = [
    "campaignName", 
    "adGroupName", 
    "keyword", 
    "keywordBid", 
    "New Bid", 
    "ACOS_30d", 
    "ACOS_7d", 
    "clicks_7d", 
    "action_desc"
]
result_df = pd.DataFrame(results, columns=columns)

# Save the results to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_1_IT_2024-06-21.csv'
result_df.to_csv(output_path, index=False)

print(f"Results have been saved to {output_path}")