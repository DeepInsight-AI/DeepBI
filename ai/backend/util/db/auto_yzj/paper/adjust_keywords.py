# filename: adjust_keywords.py

import pandas as pd

def adjust_bid(current_bid, avg_acos_7d, min_bid=0.05):
    new_bid = current_bid / ((avg_acos_7d - 0.24) / 0.24 + 1)
    return max(new_bid, min_bid)

def lower_bid(current_bid, reduction, min_bid=0.05):
    new_bid = current_bid - reduction
    return max(new_bid, min_bid)

# Load the CSV file
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\预处理.csv"
data = pd.read_csv(file_path)

# Initialize the list to store results
results = []

# Iterate through each row and apply the given definitions
for index, row in data.iterrows():
    keywordId = row['keywordId']
    keyword = row['keyword']
    targeting = row['targeting']
    keywordBid = row['keywordBid']
    matchType = row['matchType']
    adGroupName = row['adGroupName']
    campaignName = row['campaignName']
    ORDER_1m = row['ORDER_1m']
    total_clicks_30d = row['total_clicks_30d']
    total_clicks_7d = row['total_clicks_7d']
    total_clicks_yesterday = row['total_clicks_yesterday']
    total_sales14d_30d = row['total_sales14d_30d']
    total_sales14d_7d = row['total_sales14d_7d']
    total_sales14d_3d = row['total_sales14d_3d']
    total_sales14d_yesterday = row['total_sales14d_yesterday']
    total_cost_30d = row['total_cost_30d']
    total_cost_7d = row['total_cost_7d']
    total_cost_3d = row['total_cost_3d']
    total_cost_yesterday = row['total_cost_yesterday']
    ACOS_30d = row['ACOS_30d']
    ACOS_7d = row['ACOS_7d']
    ACOS_3d = row['ACOS_3d']
    ACOS_yesterday = row['ACOS_yesterday']
    
    reason = ''
    new_keywordBid = keywordBid

    if 0.24 < ACOS_7d <= 0.5 and 0 < ACOS_30d <= 0.5 and ORDER_1m < 5 and ACOS_3d >= 0.24:
        reason = '定义一'
        new_keywordBid = adjust_bid(keywordBid, ACOS_7d)
        
    elif ACOS_7d > 0.5 and ACOS_30d <= 0.36 and ACOS_3d > 0.24:
        reason = '定义五'
        new_keywordBid = adjust_bid(keywordBid, ACOS_7d)
        
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and total_cost_7d <= 5 and ACOS_30d <= 0.36:
        reason = '定义三'
        new_keywordBid = lower_bid(keywordBid, 0.03)
        
    elif total_clicks_7d > 10 and total_sales14d_7d == 0 and total_cost_7d > 7 and ACOS_30d > 0.5:
        reason = '定义四'
        new_keywordBid = max(0.05, new_keywordBid)
        
    elif ACOS_7d > 0.5 and ACOS_3d > 0.24 and ACOS_30d > 0.36:
        reason = '定义五'
        new_keywordBid = max(0.05, new_keywordBid)
        
    elif total_sales14d_30d == 0 and total_cost_30d >= 10 and total_clicks_30d >= 15:
        reason = '定义六'
        new_keywordBid = max(0.05, new_keywordBid)

    elif 0.24 < ACOS_7d <= 0.5 and 0 < ACOS_30d <= 0.5 and ORDER_1m < 5 and total_sales14d_3d == 0:
        reason = '定义七'
        new_keywordBid = adjust_bid(keywordBid, ACOS_7d)
        
    elif ACOS_7d > 0.5 and ACOS_30d <= 0.36 and total_sales14d_3d == 0:
        reason = '定义八'
        new_keywordBid = adjust_bid(keywordBid, ACOS_7d)
        
    elif ACOS_7d > 0.5 and total_sales14d_3d == 0 and ACOS_30d > 0.36:
        reason = '定义九'
        new_keywordBid = max(0.05, new_keywordBid)
        
    elif 0.24 < ACOS_7d <= 0.5 and ACOS_30d > 0.5 and ORDER_1m < 5 and ACOS_3d >= 0.24:
        reason = '定义十'
        new_keywordBid = adjust_bid(keywordBid, ACOS_7d)
        
    elif 0.24 < ACOS_7d <= 0.5 and total_sales14d_3d == 0 and ACOS_30d > 0.5:
        reason = '定义十一'
        new_keywordBid = adjust_bid(keywordBid, ACOS_7d)
        
    elif ACOS_7d <= 0.24 and total_sales14d_3d == 0 and 3 < total_cost_3d < 5:
        reason = '定义十二'
        new_keywordBid = lower_bid(keywordBid, 0.01)
        
    elif ACOS_7d <= 0.24 and 0.24 < ACOS_3d < 0.36:
        reason = '定义十三'
        new_keywordBid = lower_bid(keywordBid, 0.02)
        
    elif ACOS_7d <= 0.24 and ACOS_3d > 0.36:
        reason = '定义十四'
        new_keywordBid = lower_bid(keywordBid, 0.03)
        
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and total_cost_7d >= 10 and ACOS_30d <= 0.36:
        reason = '定义十五'
        new_keywordBid = max(0.05, new_keywordBid)
        
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and 5 < total_cost_7d < 10 and ACOS_30d <= 0.36:
        reason = '定义十六'
        new_keywordBid = lower_bid(keywordBid, 0.07)
    
    # If a rule was triggered, append the row to the results list. Add the calculated columns.
    if reason:
        results.append([
            keyword, keywordId, campaignName, adGroupName, matchType, keywordBid, new_keywordBid, targeting,
            total_cost_yesterday, total_clicks_yesterday, total_cost_7d, total_sales14d_7d, total_cost_7d, ACOS_7d,
            ACOS_30d, ACOS_3d, total_clicks_30d, reason
        ])

# Create a DataFrame for the results
columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'new_keywordBid',
           'targeting', 'cost_yesterday', 'clicks_yesterday', 'total_cost_7d', 'total_sales14d_7d', 'ad_group_cost_7d',
           'ACOS_7d', 'ACOS_30d', 'ACOS_3d', 'total_clicks_30d', 'reason']
results_df = pd.DataFrame(results, columns=columns)

# Save the results to a CSV file
output_file = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\提问策略\\手动_ASIN_劣质商品投放_v1_1_LAPASA_UK_2024-07-14.csv"
results_df.to_csv(output_file, index=False)

print("Results have been saved to", output_file)