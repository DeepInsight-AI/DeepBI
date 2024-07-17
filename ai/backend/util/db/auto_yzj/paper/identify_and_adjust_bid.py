# filename: identify_and_adjust_bid.py

import pandas as pd
import numpy as np

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义新字段，用于存储新的出价
data['New_keywordBid'] = data['keywordBid']

# 定义函数更新出价
def adjust_bid(row, new_bid):
    if new_bid < 0.05:
        return row['keywordBid'] if row['keywordBid'] < 0.05 else 0.05
    return new_bid

# 根据给定的多个定义条件调整出价
for idx, row in data.iterrows():
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5 and row['ORDER_1m'] < 5 and row['ACOS_3d'] >= 0.24:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36 and row['ACOS_3d'] >= 0.24:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] <= 5 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] - 0.03
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] > 7 and row['ACOS_30d'] > 0.5:
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, 0.05)

    elif row['ACOS_7d'] > 0.5 and row['ACOS_3d'] > 0.24 and row['ACOS_30d'] > 0.36:
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, 0.05)

    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 10 and row['total_clicks_30d'] >= 15:
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, 0.05)

    elif 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5 and row['ORDER_1m'] < 5 and row['total_sales14d_3d'] == 0:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36 and row['total_sales14d_3d'] == 0:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['ACOS_7d'] > 0.5 and row['total_sales14d_3d'] == 0 and row['ACOS_30d'] > 0.36:
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, 0.05)

    elif 0.24 < row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0.5 and row['ORDER_1m'] < 5 and row['ACOS_3d'] >= 0.24:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif 0.24 < row['ACOS_7d'] <= 0.5 and row['total_sales14d_3d'] == 0 and row['ACOS_30d'] > 0.5:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['ACOS_7d'] <= 0.24 and row['total_sales14d_3d'] == 0 and 3 < row['total_cost_3d'] < 5:
        new_bid = row['keywordBid'] - 0.01
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['ACOS_7d'] <= 0.24 and 0.24 < row['ACOS_3d'] < 0.36:
        new_bid = row['keywordBid'] - 0.02
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['ACOS_7d'] <= 0.24 and row['ACOS_3d'] > 0.36:
        new_bid = row['keywordBid'] - 0.03
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] >= 10 and row['ACOS_30d'] <= 0.36:
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, 0.05)

    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and 5 < row['total_cost_7d'] < 10 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] - 0.07
        data.at[idx, 'New_keywordBid'] = adjust_bid(row, new_bid)

# 保存到新的CSV文件中
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_UK_2024-07-11.csv'

result_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid',
    'targeting', 'total_cost_yesterday', 'total_clicks_yesterday', 'total_cost_7d', 'total_sales14d_7d',
    'total_cost_3d', 'ACOS_7d', 'ACOS_30d', 'ACOS_3d', 'total_clicks_30d'
]

data[result_columns].to_csv(output_path, index=False)
print("CSV file has been saved successfully.")