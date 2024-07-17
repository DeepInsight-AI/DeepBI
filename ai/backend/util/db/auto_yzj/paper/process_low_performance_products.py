# filename: process_low_performance_products.py

import pandas as pd

# 定义文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_DELOMO_DE_2024-07-09.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 初始化需要输出的列
result_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_7d', 
    'total_sales14d_7d', 'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', 'action', 'reason'
]

results = []

# 定义一到七：
for index, row in df.iterrows():
    action = None
    reason = None
    new_keyword_bid = row['keywordBid']  # 默认是现出价

    # 定义一
    if (0.24 < row['ACOS_7d'] <= 0.5) and (0 < row['ACOS_30d'] <= 0.5):
        new_keyword_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        action = 'Adjust Bid'
        reason = '定义一'
    
    # 定义二
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] <= 0.36):
        new_keyword_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        action = 'Adjust Bid'
        reason = '定义二'
        
    # 定义三
    elif (row['total_clicks_7d'] >= 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] <= 0.36):
        new_keyword_bid -= 0.04
        action = 'Lower Bid'
        reason = '定义三'
    
    # 定义四
    elif (row['total_clicks_7d'] > 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] > 0.5):
        new_keyword_bid = '关闭'
        action = 'Close'
        reason = '定义四'
    
    # 定义五
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] > 0.36):
        new_keyword_bid = '关闭'
        action = 'Close'
        reason = '定义五'
        
    # 定义六
    elif (row['total_sales14d_30d'] == 0) and (row['total_cost_30d'] >= 5):
        new_keyword_bid = '关闭'
        action = 'Close'
        reason = '定义六'
        
    # 定义七
    elif (row['total_sales14d_30d'] == 0) and (row['total_clicks_30d'] >= 15) and (row['total_clicks_7d'] > 0):
        new_keyword_bid = '关闭'
        action = 'Close'
        reason = '定义七'
        
    # 如果符合任何一个定义，则加入结果
    if action:
        results.append([
            row['keyword'], row['keywordId'], row['campaignName'], row['adGroupName'], row['matchType'],
            row['keywordBid'], new_keyword_bid, row['targeting'], row['total_cost_7d'],
            row['total_sales14d_7d'], row['ACOS_7d'], row['ACOS_30d'], row['total_clicks_30d'], action, reason
        ])

# 转换为DataFrame并输出到CSV文件
result_df = pd.DataFrame(results, columns=result_columns)
result_df.to_csv(output_file, index=False)

print(f"结果已保存在 {output_file}")