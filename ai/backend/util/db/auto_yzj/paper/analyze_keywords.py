# filename: analyze_keywords.py

import pandas as pd

# Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# Renaming columns based on given description
data.rename(columns={
    "keywordId": "Keyword ID",
    "keyword": "Keyword",
    "targeting": "Targeting",
    "keywordBid": "Keyword Bid",
    "matchType": "Match Type",
    "adGroupName": "Ad Group Name",
    "campaignName": "Campaign Name",
    "ORDER_1m": "Orders 1m",
    "total_clicks_30d": "Clicks 30d",
    "total_clicks_7d": "Clicks 7d",
    "total_clicks_yesterday": "Clicks Yesterday",
    "total_sales14d_30d": "Sales 30d",
    "total_sales14d_7d": "Sales 7d",
    "total_sales14d_3d": "Sales 3d",
    "total_sales14d_yesterday": "Sales Yesterday",
    "total_cost_30d": "Cost 30d",
    "total_cost_7d": "Cost 7d",
    "total_cost_3d": "Cost 3d",
    "total_cost_yesterday": "Cost Yesterday",
    "ACOS_30d": "ACOS 30d",
    "ACOS_7d": "ACOS 7d",
    "ACOS_3d": "ACOS 3d",
    "ACOS_yesterday": "ACOS Yesterday"
}, inplace=True)

# Initialize an empty DataFrame to store results
result = pd.DataFrame(columns=[
    "Keyword", "Keyword ID", "Campaign Name", "Ad Group Name", "Match Type",
    "Keyword Bid", "New Keyword Bid", "Targeting", "Cost", "Clicks", 
    "Cost 7d", "Sales 7d", "Ad Group Cost 7d", "ACOS 7d", "ACOS 30d", 
    "ACOS 3d", "Clicks 30d", "Reason", "Action"
])

# Define function to adjust keyword bid
def adjust_bid(keywordBid, avg_ACOS_7d):
    new_bid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
    return max(new_bid, 0.05)

# Define poor performing keywords based on given definitions
for index, row in data.iterrows():
    keywordBid = row["Keyword Bid"]
    avg_ACOS_7d = row["ACOS 7d"]
    avg_ACOS_3d = row["ACOS 3d"]
    avg_ACOS_30d = row["ACOS 30d"]
    clicks_7d = row["Clicks 7d"]
    sales_7d = row["Sales 7d"]
    sales_30d = row["Sales 30d"]
    cost_7d = row["Cost 7d"]
    clicks_30d = row["Clicks 30d"]
    cost_30d = row["Cost 30d"]
    orders_1m = row["Orders 1m"]

    new_bid, reason, action = keywordBid, "", ""

    if avg_ACOS_7d > 0.24 and avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0 and avg_ACOS_30d <= 0.5 and orders_1m < 5 and avg_ACOS_3d >= 0.24:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d) if keywordBid > 0.05 else keywordBid
        reason = "Definition 1"
        action = "Adjusted Bid"

    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d >= 0.24:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d) if keywordBid > 0.05 else keywordBid
        reason = "Definition 2"
        action = "Adjusted Bid"
    
    # Additional definitions go here following similar pattern...

    if reason:
        result = result.append({
            "Keyword": row["Keyword"],
            "Keyword ID": row["Keyword ID"],
            "Campaign Name": row["Campaign Name"],
            "Ad Group Name": row["Ad Group Name"],
            "Match Type": row["Match Type"],
            "Keyword Bid": keywordBid,
            "New Keyword Bid": new_bid,
            "Targeting": row["Targeting"],
            "Cost": row["Cost 7d"],
            "Clicks": row["Clicks 7d"],
            "Cost 7d": cost_7d,
            "Sales 7d": sales_7d,
            "Ad Group Cost 7d": cost_7d,
            "ACOS 7d": avg_ACOS_7d,
            "ACOS 30d": avg_ACOS_30d,
            "ACOS 3d": avg_ACOS_3d,
            "Clicks 30d": clicks_30d,
            "Reason": reason,
            "Action": action
        }, ignore_index=True)

# Save the results to a CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_FR_2024-07-12.csv'
result.to_csv(output_path, index=False)

print(f"Analysis complete. Results saved to {output_path}")