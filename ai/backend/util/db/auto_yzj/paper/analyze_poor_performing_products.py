# filename: analyze_poor_performing_products.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义基础函数
def calculate_new_bid(row):
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        return row['keywordBid'] - 0.04
    else:
        return row['keywordBid']

def should_close(row):
    if row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        return '关闭'
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        return '关闭'
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        return '关闭'
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        return '关闭'
    else:
        return ''

# 计算新关键词竞价
data['New_keywordBid'] = data.apply(calculate_new_bid, axis=1)

# 识别需要关闭的商品投放
data['操作'] = data.apply(should_close, axis=1)

# 将需要关闭的商品投放的新竞价标记为'关闭'
data.loc[data['操作'] == '关闭', 'New_keywordBid'] = '关闭'

# 筛选被识别的商品投放
filtered_data = data[
    (0.24 < data['ACOS_7d']) & (data['ACOS_7d'] <= 0.5) & (0 < data['ACOS_30d']) & (data['ACOS_30d'] <= 0.5) |
    (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36) |
    (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36) |
    (data['total_clicks_7d'] > 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5) |
    (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36) |
    (data['total_sales14d_30d'] == 0) & (data['total_cost_30d'] >= 5) |
    (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15) & (data['total_clicks_7d'] > 0)
]

# 保留需要的列
output_data = filtered_data[[
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
    'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 'total_clicks_7d', 
    'total_sales14d_7d', 'total_sales14d_30d', 'total_cost_7d', 'ACOS_7d', 'ACOS_30d', 'total_clicks_7d', '操作'
]]

# 输出结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_OutdoorMaster_ES_2024-07-09.csv'
output_data.to_csv(output_path, index=False)

print(f"Filtered data has been saved to {output_path}")