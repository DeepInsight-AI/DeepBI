# filename: low_performance_ads_processing.py

import pandas as pd

# Load the CSV data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# Create an empty list to hold results
results = []

# Iterate through each row to apply the rules
for index, row in data.iterrows():
    keyword = row['keyword']
    keywordId = row['keywordId']
    campaignName = row['campaignName']
    adGroupName = row['adGroupName']
    matchType = row['matchType']
    keywordBid = row['keywordBid']
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    sales_7d = row['total_sales14d_7d']
    sales_30d = row['total_sales14d_30d']
    clicks_7d = row['total_clicks_7d']
    cost_7d = row['total_cost_7d']
    clicks_30d = row['total_clicks_30d']
    cost_30d = row['total_cost_30d']
    targeting = row['targeting']
    
    new_keywordBid = keywordBid
    action = ""
    reason = ""

    # Definition 1
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "Adjust Bid"
        reason = "Definition 1: 0.24 < ACOS_7d <= 0.5, 0 < ACOS_30d <= 0.5"

    # Definition 2
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "Adjust Bid"
        reason = "Definition 2: ACOS_7d > 0.5, ACOS_30d <= 0.36"

    # Definition 3
    elif clicks_7d >= 10 and sales_7d == 0 and avg_ACOS_30d <= 0.36:
        new_keywordBid = keywordBid - 0.04
        action = "Lower Bid by 0.04"
        reason = "Definition 3: clicks_7d >= 10, sales_7d == 0, ACOS_30d <= 0.36"

    # Definition 4
    elif clicks_7d >= 10 and sales_7d == 0 and avg_ACOS_30d > 0.5:
        new_keywordBid = "关闭"
        action = "Close Keyword"
        reason = "Definition 4: clicks_7d >= 10, sales_7d == 0, ACOS_30d > 0.5"

    # Definition 5
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d > 0.36:
        new_keywordBid = "关闭"
        action = "Close Keyword"
        reason = "Definition 5: ACOS_7d > 0.5, ACOS_30d > 0.36"

    # Definition 6
    elif sales_30d == 0 and cost_30d >= 5:
        new_keywordBid = "关闭"
        action = "Close Keyword"
        reason = "Definition 6: sales_30d == 0, cost_30d >= 5"

    # Definition 7
    elif sales_30d == 0 and clicks_30d >= 15 and clicks_7d > 0:
        new_keywordBid = "关闭"
        action = "Close Keyword"
        reason = "Definition 7: sales_30d == 0, clicks_30d >= 15, clicks_7d > 0"

    # Add results to list if any action is needed
    if action:
        results.append({
            "keyword": keyword,
            "keywordId": keywordId,
            "campaignName": campaignName,
            "adGroupName": adGroupName,
            "matchType": matchType,
            "keywordBid": keywordBid,
            "new_keywordBid": new_keywordBid,
            "targeting": targeting,
            "cost_7d": cost_7d,
            "sales_7d": sales_7d,
            "cost_7d": cost_7d,
            "avg_ACOS_7d": avg_ACOS_7d,
            "avg_ACOS_30d": avg_ACOS_30d,
            "clicks_30d": clicks_30d,
            "action": action,
            "reason": reason
        })

# Create DataFrame from results
results_df = pd.DataFrame(results)

# Save results to CSV
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_ES_2024-07-04.csv'
results_df.to_csv(output_file, index=False)

print("完成：处理结果已保存到CSV文件中。")