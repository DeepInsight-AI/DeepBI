# filename: process_low_performance_keywords_v2.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\预处理.csv"
data = pd.read_csv(file_path)

# 定义新列存储新竞价或关闭状态
data['New_keywordBid'] = data['keywordBid']

# 定义调整新竞价或关闭规则函数
def adjust_bid_or_close(row):
    if (row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5) and (row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.5):
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] <= 0.36):
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif (row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] <= 0.36):
        return row['keywordBid'] - 0.04
    elif (row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] > 0.5):
        return '关闭'
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] > 0.36):
        return '关闭'
    elif (row['total_sales14d_30d'] == 0) and (row['total_cost_30d'] >= 5):
        return '关闭'
    elif (row['total_sales14d_30d'] == 0) and (row['total_clicks_30d'] >= 15) and (row['total_clicks_7d'] > 0):
        return '关闭'
    return row['keywordBid']

# 应用规则到每一行
data['New_keywordBid'] = data.apply(adjust_bid_or_close, axis=1)

# 定义输出需要的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_30d',
    'total_clicks_30d', 'total_cost_7d', 'total_sales14d_7d',
    'ACOS_7d', 'ACOS_30d', 'total_clicks_30d'
]

# 筛选出需要关闭或者调整竞价的商品投放
filtered_data = data[
    (data['New_keywordBid'] != data['keywordBid'])
]

# 保存到新CSV文件
output_file = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\提问策略\\手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-09.csv"
filtered_data.to_csv(output_file, columns=output_columns, index=False)

print("处理完成，结果已保存到文件:", output_file)