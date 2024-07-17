# filename: process_low_performance_keywords.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\预处理.csv"
data = pd.read_csv(file_path)

# 定义新列存储新竞价或关闭状态
data['New_keywordBid'] = data['keywordBid']

# 定义一规则
condition1 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] <= 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.5)
data.loc[condition1, 'New_keywordBid'] = data.loc[condition1, 'keywordBid'] / ((data.loc[condition1, 'ACOS_7d'] - 0.24) / 0.24 + 1)

# 定义二规则
condition2 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36)
data.loc[condition2, 'New_keywordBid'] = data.loc[condition2, 'keywordBid'] / ((data.loc[condition2, 'ACOS_7d'] - 0.24) / 0.24 + 1)

# 定义三规则
condition3 = (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36)
data.loc[condition3, 'New_keywordBid'] = data.loc[condition3, 'keywordBid'] - 0.04

# 定义四规则
condition4 = (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5)
data.loc[condition4, 'New_keywordBid'] = '关闭'

# 定义五规则
condition5 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36)
data.loc[condition5, 'New_keywordBid'] = '关闭'

# 定义六规则
condition6 = (data['total_sales14d_30d'] == 0) & (data['total_cost_30d'] >= 5)
data.loc[condition6, 'New_keywordBid'] = '关闭'

# 定义七规则
condition7 = (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15) & (data['total_clicks_7d'] > 0)
data.loc[condition7, 'New_keywordBid'] = '关闭'

# 输出需要的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_30d', 
    'total_clicks_30d', 'total_cost_7d', 'total_sales14d_7d', 
    'ACOS_7d', 'ACOS_30d', 'total_clicks_30d'
]

# 筛选出需要关闭或者调整竞价的商品投放
filtered_data = data[
    condition1 | condition2 | condition3 | condition4 | condition5 | condition6 | condition7
]

# 保存到新CSV文件
output_file = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\提问策略\\手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-09.csv"
filtered_data.to_csv(output_file, columns=output_columns, index=False)

print("处理完成，结果已保存到文件:", output_file)