# filename: preprocess_and_filter_ads.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义ACOS和BID的计算方法
def calculate_new_bid(keywordBid, avg_ACOS_7d, threshold):
    return keywordBid / (((avg_ACOS_7d - threshold) / threshold) + 1)

# 初始化新列
data['New_keywordBid'] = ""
data['Action_Reason'] = ""

for i, row in data.iterrows():
    new_bid = None
    action_reason = ""

    if (0.24 < row['ACOS_7d'] <= 0.5) and (0 < row['ACOS_30d'] <= 0.5):
        new_bid = calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24)
        action_reason = "定义一: 调整竞价"
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] <= 0.36):
        new_bid = calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24)
        action_reason = "定义二: 调整竞价"
    elif (row['total_clicks_7d'] >= 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] <= 0.36):
        new_bid = row['keywordBid'] - 0.04
        action_reason = "定义三: 降低竞价"
    elif (row['total_clicks_7d'] >= 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] > 0.5):
        new_bid = "关闭"
        action_reason = "定义四: 关闭"
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] > 0.36):
        new_bid = "关闭"
        action_reason = "定义五: 关闭"
    elif (row['total_sales14d_30d'] == 0) and (row['total_cost_30d'] >= 5):
        new_bid = "关闭"
        action_reason = "定义六: 关闭"
    elif (row['total_sales14d_30d'] == 0) and (row['total_clicks_30d'] >= 15) and (row['total_clicks_7d'] > 0):
        new_bid = "关闭"
        action_reason = "定义七: 关闭"
    
    if new_bid is not None:
        data.at[i, 'New_keywordBid'] = new_bid
        data.at[i, 'Action_Reason'] = action_reason

# 筛选出需要调整的商品投放
result = data[(data['New_keywordBid'] != "") & (data['Action_Reason'] != "")]

# 选取需要的列
result = result[[
    'keyword',
    'keywordId',
    'campaignName',
    'adGroupName',
    'matchType',
    'keywordBid',
    'New_keywordBid',
    'targeting',
    'total_cost_yesterday',
    'total_clicks_yesterday',
    'total_cost_7d',
    'total_sales14d_7d',
    'total_cost_yesterday',
    'ACOS_7d',
    'ACOS_30d',
    'total_clicks_30d',
    'Action_Reason'
]]

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_ITES_2024-07-02.csv'
result.to_csv(output_path, index=False)
print("过滤并输出结果完毕！")