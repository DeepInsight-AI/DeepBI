# filename: analyze_campaign_performance.py

import pandas as pd

# 定义读取文件路径和保存文件路径
input_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/预处理.csv"
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/提问策略/手动_ASIN_劣质商品投放_v1_1_DE_2024-06-30.csv"

# 读取 CSV 文件
df = pd.read_csv(input_file_path)

# 定义计算新竞价的方法
def calculate_new_keyword_bid(row):
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        return row['keywordBid'] - 0.04
    else:
        return row['keywordBid']

# 筛选需要调整或关闭的商品投放
def identify_poor_performance(row):
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        return '调整竞价 ' + str(calculate_new_keyword_bid(row))
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        return '调整竞价 ' + str(calculate_new_keyword_bid(row))
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        return '降低竞价0.04'
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        return '关闭'
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        return '关闭'
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        return '关闭'
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        return '关闭'
    return '保留'

# 计算新的投放竞价
df['New_keywordBid'] = df.apply(lambda row: calculate_new_keyword_bid(row) if identify_poor_performance(row).startswith('调整') else '关闭' if identify_poor_performance(row) == '关闭' else row['keywordBid'], axis=1)

# 识别劣质商品投放
df['操作'] = df.apply(identify_poor_performance, axis=1)

# 筛选需要输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid',
    'targeting', 'total_cost_yesterday', 'total_clicks_yesterday', 'total_cost_7d', 'total_sales14d_yesterday',
    'total_cost_7d', 'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', '操作'
]

output_df = df[df['操作'] != '保留'][output_columns]

# 保存结果到新的CSV文件
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已成功保存到 {output_file_path}")