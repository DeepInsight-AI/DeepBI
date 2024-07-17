# filename: perform_bid_increase.py

import pandas as pd

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义筛选条件并且提价
conditions_bid_increase = [
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) & (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & (data['ACOS_30d'] <= 0.1) & (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & (data['ACOS_30d'] <= 0.1) & (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & (data['ORDER_1m'] >= 2) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
]

increment_values = [0.05, 0.03, 0.04, 0.02, 0.02, 0.01]
reasons = [
    "定义一 - 提价0.05",
    "定义二 - 提价0.03",
    "定义三 - 提价0.04",
    "定义四 - 提价0.02",
    "定义五 - 提价0.02",
    "定义六 - 提价0.01",
]

# 初始化新列
data['New_keywordBid'] = data['keywordBid']
data['提高竞价'] = 0.0
data['提价原因'] = ""

for condition, increment, reason in zip(conditions_bid_increase, increment_values, reasons):
    data.loc[condition, 'New_keywordBid'] = data['keywordBid'] + increment
    data.loc[condition, '提高竞价'] = increment
    data.loc[condition, '提价原因'] = reason

# 筛选出需要的字段
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid',
    'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d',
    'ACOS_30d', 'ORDER_1m', '提高竞价', '提价原因'
]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_DE_2024-07-15.csv'
data.to_csv(output_file_path, columns=output_columns, index=False)

print("CSV file has been saved successfully.")