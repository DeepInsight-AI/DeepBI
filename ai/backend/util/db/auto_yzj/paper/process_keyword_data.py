# filename: process_keyword_data.py

import pandas as pd
import os

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
data = pd.read_csv(file_path)

# 计算广告组最近7天的总花费
ad_group_cost_7d = data.groupby('adGroupName')['total_cost_7d'].sum().reset_index()
ad_group_cost_7d.columns = ['adGroupName', 'adGroupCost7d']

# 将广告组总花费合并到主表中
data = pd.merge(data, ad_group_cost_7d, on='adGroupName', how='left')

# 定义判断函数
def keyword_analysis(row):
    reasons = []
    new_bid = row['keywordBid']
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reasons.append('定义一')
    if row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reasons.append('定义二')
    if row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] - 0.04
        reasons.append('定义三')
    if row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        new_bid = '关闭'
        reasons.append('定义四')
    if row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        new_bid = '关闭'
        reasons.append('定义五')
    if row['total_sales14d_30d'] == 0 and row['total_cost_7d'] > row['adGroupCost7d'] / 5:
        new_bid = '关闭'
        reasons.append('定义六')
    if row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15:
        new_bid = '关闭'
        reasons.append('定义七')
    return new_bid, '|'.join(reasons)

# 过滤并计算新竞价
result = data.apply(keyword_analysis, axis=1)
data['new_keywordBid'], data['reason'] = zip(*result)

# 筛选出需要调整或关闭的关键词
filtered_data = data[data['reason'] != '']

# 选择要输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'new_keywordBid',
    'targeting', 'total_cost_7d', 'total_sales14d_7d', 'adGroupCost7d', 'ACOS_7d', 'ACOS_30d', 'reason'
]
filtered_data = filtered_data[output_columns]

# 保存筛选后的结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_劣质关键词_v1_1_ES_2024-06-121.csv'
filtered_data.to_csv(output_file_path, index=False)

print(f"处理完成，筛选后的关键词已保存至 {output_file_path}")