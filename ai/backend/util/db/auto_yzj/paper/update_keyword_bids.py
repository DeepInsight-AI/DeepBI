# filename: update_keyword_bids.py

import pandas as pd

# 读取数据文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 计算辅助字段
df['avg_ACOS_7d'] = df['ACOS_7d']
df['avg_ACOS_3d'] = df['ACOS_3d']
df['avg_ACOS_30d'] = df['ACOS_30d']
df['new_keywordBid'] = df['keywordBid']

# 定义降价函数
def adjust_bid(bid, condition):
    new_bid = bid / ((condition - 0.24) / 0.24 + 1)
    return max(new_bid, 0.05)

# 应用不同的定义条件来过滤数据并调整竞价
conditions = [
    # 定义条件一
    (df['avg_ACOS_7d'] > 0.24) & (df['avg_ACOS_7d'] <= 0.5) & 
    (df['avg_ACOS_30d'] > 0) & (df['avg_ACOS_30d'] <= 0.5) & 
    (df['ORDER_1m'] < 5) & (df['avg_ACOS_3d'] >= 0.24),
    
    # 定义条件二
    (df['avg_ACOS_7d'] > 0.5) & 
    (df['avg_ACOS_30d'] <= 0.36) & 
    (df['avg_ACOS_3d'] >= 0.24),
    
    # 定义条件三
    (df['total_clicks_7d'] >= 10) & 
    (df['total_sales14d_7d'] == 0) & 
    (df['total_cost_7d'] <= 5) & 
    (df['avg_ACOS_30d'] <= 0.36)
    # 定义其他条件依次添加...
]

actions = [
    lambda row: adjust_bid(row['keywordBid'], row['avg_ACOS_7d']),
    lambda row: adjust_bid(row['keywordBid'], row['avg_ACOS_7d']),
    lambda row: max(row['keywordBid'] - 0.03, 0.05)
    # 对应其他条件的actions 依次添加...
]

# 遍历所有条件并应用
for condition, action in zip(conditions, actions):
    df.loc[condition, 'new_keywordBid'] = df.loc[condition].apply(action, axis=1)

# 生成输出文件
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'new_keywordBid', 'targeting', 'total_cost_7d', 'total_clicks_7d', 
    'total_sales14d_7d', 'avg_ACOS_7d', 'avg_ACOS_30d', 'avg_ACOS_3d', 'ORDER_1m'
    # 添加其他需要的列...
]

output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_UK_2024-07-15.csv'
df[output_columns].to_csv(output_file, index=False)

print("处理完成，输出文件保存在:", output_file)