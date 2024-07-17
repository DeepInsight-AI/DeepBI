# filename: low_performing_items_analysis.py

import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义新列用于存储新竞价或关闭状态
data['New_keywordBid'] = None
data['调整原因'] = None

# 定义一
condition_1 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] <= 0.5) & \
              (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.5)

data.loc[condition_1, 'New_keywordBid'] = data['keywordBid'] / \
                                          ((data['ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[condition_1, '调整原因'] = '调整为定义一'

# 定义二
condition_2 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36)

data.loc[condition_2, 'New_keywordBid'] = data['keywordBid'] / \
                                          ((data['ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[condition_2, '调整原因'] = '调整为定义二'

# 定义三
condition_3 = (data['total_clicks_7d'] >= 10) & \
              (data['total_sales14d_7d'] == 0) & \
              (data['ACOS_30d'] <= 0.36)

data.loc[condition_3, 'New_keywordBid'] = data['keywordBid'] - 0.04
data.loc[condition_3, '调整原因'] = '调整为定义三'

# 定义四
condition_4 = (data['total_clicks_7d'] >= 10) & \
              (data['total_sales14d_7d'] == 0) & \
              (data['ACOS_30d'] > 0.5)

data.loc[condition_4, 'New_keywordBid'] = '关闭'
data.loc[condition_4, '调整原因'] = '调整为定义四'

# 定义五
condition_5 = (data['ACOS_7d'] > 0.5) & \
              (data['ACOS_30d'] > 0.36)

data.loc[condition_5, 'New_keywordBid'] = '关闭'
data.loc[condition_5, '调整原因'] = '调整为定义五'

# 定义六
condition_6 = (data['total_sales14d_30d'] == 0) & \
              (data['total_cost_30d'] >= 5)

data.loc[condition_6, 'New_keywordBid'] = '关闭'
data.loc[condition_6, '调整原因'] = '调整为定义六'

# 定义七
condition_7 = (data['total_sales14d_30d'] == 0) & \
              (data['total_clicks_30d'] >= 15) & \
              (data['total_clicks_7d'] > 0)

data.loc[condition_7, 'New_keywordBid'] = '关闭'
data.loc[condition_7, '调整原因'] = '调整为定义七'

# 筛选出表现较差的商品投放
poor_performance_data = data[~data['New_keywordBid'].isnull()]

# 选择所需的列
result_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_30d', 
    'total_clicks_30d', 'total_clicks_7d', 'total_sales14d_7d', 
    'ACOS_30d', 'ACOS_7d', '调整原因'
]
output_data = poor_performance_data[result_columns]

# 保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_ES_2024-07-03.csv'
output_data.to_csv(output_file_path, index=False)

print(f"分析结果已保存到：{output_file_path}")