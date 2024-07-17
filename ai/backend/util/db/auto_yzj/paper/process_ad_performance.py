# filename: process_ad_performance.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
data = pd.read_csv(file_path)

# 定义提价条件
def update_bid(row):
    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.05
        row['提价原因'] = '定义一'
    elif 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.03
        row['提价原因'] = '定义二'
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.04
        row['提价原因'] = '定义三'
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.02
        row['提价原因'] = '定义四'
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.02
        row['提价原因'] = '定义五'
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.01
        row['提价原因'] = '定义六'
    return row

# 更新数据并筛选出符合条件的行
data['New_keywordBid'] = data['keywordBid']
data['提价原因'] = ''
data = data.apply(update_bid, axis=1)
filtered_data = data[data['提价原因'] != '']

# 输出结果
result_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_IT_2024-06-30.csv"
filtered_data.to_csv(result_file_path, index=False, columns=[
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid',
    'targeting', 'total_cost_30d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'New_keywordBid', '提价原因'])
print(f"Filtered data with the bidding strategy saved to {result_file_path}")