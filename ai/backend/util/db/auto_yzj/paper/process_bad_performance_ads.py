# filename: process_bad_performance_ads.py

import pandas as pd

# 读取CSV文件
input_filepath = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_filepath = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_劣质商品投放_v1_1_IT_2024-06-26.csv'

# 加载数据
df = pd.read_csv(input_filepath)

# 创建一个新的DataFrame来存储结果
result = pd.DataFrame(columns=[
    "keyword", "keywordId", "campaignName", "adGroupName", "matchType", 
    "keywordBid", "new_keywordBid", "targeting", "total_cost_7d", 
    "total_sales14d_7d", "total_cost_30d", "total_clicks_7d", 
    "ACOS_7d", "ACOS_30d", "total_clicks_30d", "action_reason"
])

# 定义1和定义2公式
def calculate_new_bid(keywordBid, avg_ACOS_7d):
    return keywordBid / (((avg_ACOS_7d - 0.24) / 0.24) + 1)

# 遍历每一行，判断是否符合条件
for index, row in df.iterrows():
    keywordBid = row['keywordBid']
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    total_clicks_7d = row['total_clicks_7d']
    total_sales14d_7d = row['total_sales14d_7d']
    total_clicks_30d = row['total_clicks_30d']
    total_cost_30d = row['total_cost_30d']
    
    action = None
    new_bid = None
    reason = ""
    
    # 定义1
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5:
        new_bid = calculate_new_bid(keywordBid, avg_ACOS_7d)
        action = "adjust_bid"
        reason = "定义一：调低竞价"
        
    # 定义2
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36:
        new_bid = calculate_new_bid(keywordBid, avg_ACOS_7d)
        action = "adjust_bid"
        reason = "定义二：调低竞价"
        
    # 定义3
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and avg_ACOS_30d <= 0.36:
        new_bid = keywordBid - 0.04
        action = "adjust_bid"
        reason = "定义三：调低竞价0.04"
        
    # 定义4
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and avg_ACOS_30d > 0.5:
        new_bid = "关闭"
        action = "close"
        reason = "定义四：点击量多但无销售，ACOS较高，关闭"
        
    # 定义5
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d > 0.36:
        new_bid = "关闭"
        action = "close"
        reason = "定义五：ACOS长期较高，关闭"
        
    # 定义6
    elif total_sales14d_7d == 0 and total_cost_30d >= 5:
        new_bid = "关闭"
        action = "close"
        reason = "定义六：30天内没有销售但花费大于等于5，关闭"
        
    # 定义7
    elif total_sales14d_7d == 0 and total_clicks_30d >= 15 and total_clicks_7d > 0:
        new_bid = "关闭"
        action = "close"
        reason = "定义七：30天无销售但点击量大于15，关闭"
    
    if action:
        new_row = pd.DataFrame({
            "keyword": [row['keyword']],
            "keywordId": [row['keywordId']],
            "campaignName": [row['campaignName']],
            "adGroupName": [row['adGroupName']],
            "matchType": [row['matchType']],
            "keywordBid": [keywordBid],
            "new_keywordBid": [new_bid],
            "targeting": [row['targeting']],
            "total_cost_7d": [row['total_cost_7d']],
            "total_sales14d_7d": [row['total_sales14d_7d']],
            "total_cost_30d": [row['total_cost_30d']],
            "total_clicks_7d": [total_clicks_7d],
            "ACOS_7d": [avg_ACOS_7d],
            "ACOS_30d": [avg_ACOS_30d],
            "total_clicks_30d": [total_clicks_30d],
            "action_reason": [reason]
        })
        result = pd.concat([result, new_row], ignore_index=True)

# 输出结果到CSV文件
result.to_csv(output_filepath, index=False)
print("Process completed and the result has been saved to", output_filepath)