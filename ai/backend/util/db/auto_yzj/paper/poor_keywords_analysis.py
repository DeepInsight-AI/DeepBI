# filename: poor_keywords_analysis.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 初始化新的关键词竞价列
df['new_keywordBid'] = df['keywordBid']

# 定义竞价调整函数
def adjust_bid(bid, adjustment):
    new_bid = bid / adjustment
    return max(new_bid, 0.05)

# 定义所有规则及竞价调整
# 定义一
df.loc[
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.5) &
    (df['ORDER_1m'] < 5) & (df['ACOS_3d'] >= 0.24),
    'new_keywordBid'
] = df.apply(lambda row: adjust_bid(row['keywordBid'], ((row['ACOS_7d'] - 0.24) / 0.24 + 1)), axis=1)

# 定义二
df.loc[
    (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] <= 0.36) & (df['ACOS_3d'] >= 0.24),
    'new_keywordBid'
] = df.apply(lambda row: adjust_bid(row['keywordBid'], ((row['ACOS_7d'] - 0.24) / 0.24 + 1)), axis=1)

# 定义三
df.loc[
    (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['total_cost_7d'] <= 5) & (df['ACOS_30d'] <= 0.36),
    'new_keywordBid'
] = df.apply(lambda row: max(row['keywordBid'] - 0.03, 0.05), axis=1)

# 定义四
df.loc[
    (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['total_cost_7d'] > 7) & (df['ACOS_30d'] > 0.5),
    'new_keywordBid'
] = 0.05

# 定义五
df.loc[
    (df['ACOS_7d'] > 0.5) & (df['ACOS_3d'] > 0.24) & (df['ACOS_30d'] > 0.36),
    'new_keywordBid'
] = 0.05

# 定义六
df.loc[
    (df['total_sales14d_30d'] == 0) & (df['total_cost_30d'] >= 10) & (df['total_clicks_30d'] >= 15),
    'new_keywordBid'
] = 0.05

# 定义七
df.loc[
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.5) &
    (df['ORDER_1m'] < 5) & (df['total_sales14d_3d'] == 0),
    'new_keywordBid'
] = df.apply(lambda row: adjust_bid(row['keywordBid'], ((row['ACOS_7d'] - 0.24) / 0.24 + 1)), axis=1)

# 定义八
df.loc[
    (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] <= 0.36) & (df['total_sales14d_3d'] == 0),
    'new_keywordBid'
] = df.apply(lambda row: adjust_bid(row['keywordBid'], ((row['ACOS_7d'] - 0.24) / 0.24 + 1)), axis=1)

# 定义九
df.loc[
    (df['ACOS_7d'] > 0.5) & (df['total_sales14d_3d'] == 0) & (df['ACOS_30d'] > 0.36),
    'new_keywordBid'
] = 0.05

# 定义十
df.loc[
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0.5) & (df['ORDER_1m'] < 5) & 
    (df['ACOS_3d'] >= 0.24),
    'new_keywordBid'
] = df.apply(lambda row: adjust_bid(row['keywordBid'], ((row['ACOS_7d'] - 0.24) / 0.24 + 1)), axis=1)

# 定义十一
df.loc[
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['total_sales14d_3d'] == 0) & (df['ACOS_30d'] > 0.5),
    'new_keywordBid'
] = df.apply(lambda row: adjust_bid(row['keywordBid'], ((row['ACOS_7d'] - 0.24) / 0.24 + 1)), axis=1)

# 定义十二
df.loc[
    (df['ACOS_7d'] <= 0.24) & (df['total_sales14d_3d'] == 0) & (3 < df['total_cost_3d']) & (df['total_cost_3d'] < 5),
    'new_keywordBid'
] = df.apply(lambda row: max(row['keywordBid'] - 0.01, 0.05), axis=1)

# 定义十三
df.loc[
    (df['ACOS_7d'] <= 0.24) & (df['ACOS_3d'] > 0.24) & (df['ACOS_3d'] < 0.36),
    'new_keywordBid'
] = df.apply(lambda row: max(row['keywordBid'] - 0.02, 0.05), axis=1)

# 定义十四
df.loc[
    (df['ACOS_7d'] <= 0.24) & (df['ACOS_3d'] > 0.36),
    'new_keywordBid'
] = df.apply(lambda row: max(row['keywordBid'] - 0.03, 0.05), axis=1)

# 定义十五
df.loc[
    (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['total_cost_7d'] >= 10) & (df['ACOS_30d'] <= 0.36),
    'new_keywordBid'
] = 0.05

# 定义十六
df.loc[
    (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['total_cost_7d'] > 5) & (df['total_cost_7d'] < 10) & 
    (df['ACOS_30d'] <= 0.36),
    'new_keywordBid'
] = df.apply(lambda row: max(row['keywordBid'] - 0.07, 0.05), axis=1)

# 添加操作原因
df['operation_reason'] = ''
df.loc[df['keywordBid'] != df['new_keywordBid'], 'operation_reason'] = 'Reduce bid or close keyword'

# 输出字段
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'new_keywordBid', 'targeting', 'total_cost_7d',
    'total_sales14d_7d', 'total_cost_7d', 'ACOS_7d', 'ACOS_30d', 'ACOS_3d',
    'total_clicks_30d', 'operation_reason'
]

# 筛选出调整过竞价的关键词
output_df = df[df['keywordBid'] != df['new_keywordBid']][output_columns]

# 将结果保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_UK_2024-07-12.csv'
output_df.to_csv(output_file_path, index=False)

print("CSV文件已生成，路径为：", output_file_path)