# filename: optimize_ad_bids.py

import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义提价函数
def calculate_new_bid(row):
    # 定义一的条件
    if (0 < row['ACOS_7d'] <= 0.1 and 
        0 < row['ACOS_30d'] <= 0.1 and 
        row['ORDER_1m'] >= 2):
        new_bid = row['keywordBid'] + 0.05
        reason = "定义一"
    
    # 定义二的条件
    elif (0 < row['ACOS_7d'] <= 0.1 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2):
        new_bid = row['keywordBid'] + 0.03
        reason = "定义二"
    
    # 定义三的条件
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 
          0 < row['ACOS_30d'] <= 0.1 and 
          row['ORDER_1m'] >= 2):
        new_bid = row['keywordBid'] + 0.04
        reason = "定义三"
    
    # 定义四的条件
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2):
        new_bid = row['keywordBid'] + 0.02
        reason = "定义四"
    
    # 定义五的条件
    elif (0.2 < row['ACOS_7d'] <= 0.24 and
          0 < row['ACOS_30d'] <= 0.1 and
          row['ORDER_1m'] >= 2):
        new_bid = row['keywordBid'] + 0.02
        reason = "定义五"
    
    # 定义六的条件
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2):
        new_bid = row['keywordBid'] + 0.01
        reason = "定义六"
    
    else:
        new_bid = row['keywordBid']
        reason = "不符合条件"
    
    return new_bid, reason

# 计算并输出符合条件的商品投放
result = []
for idx, row in data.iterrows():
    new_bid, reason = calculate_new_bid(row)
    if reason != "不符合条件":
        result.append([
            row['keyword'], 
            row['keywordId'], 
            row['campaignName'], 
            row['adGroupName'], 
            row['matchType'], 
            row['keywordBid'], 
            new_bid, 
            row['targeting'],
            row['total_cost_30d'], 
            row['total_clicks_7d'], 
            row['ACOS_7d'], 
            row['ACOS_30d'], 
            row['ORDER_1m'], 
            new_bid - row['keywordBid'], 
            reason
        ])

# 转换为DataFrame并保存为CSV
columns = [
    "keyword", 
    "keywordId", 
    "campaignName", 
    "adGroupName", 
    "matchType", 
    "keywordBid", 
    "New_keywordBid", 
    "targeting", 
    "cost", 
    "clicks", 
    "recent7d_ACOS", 
    "month_ACOS", 
    "orders_month", 
    "raise_amount", 
    "reason"
]
result_df = pd.DataFrame(result, columns=columns)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_IT_2024-06-30.csv'
result_df.to_csv(output_path, index=False)

print("处理完成，结果已保存到", output_path)