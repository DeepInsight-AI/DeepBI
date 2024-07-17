# filename: process_inferior_products.py
import pandas as pd
import numpy as np

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 准备新列
data['New_keywordBid'] = data['keywordBid']
data['Action_Reason'] = ''

# 定义条件
# 定义1
condition1 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] <= 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.5)
data.loc[condition1, 'New_keywordBid'] = data.loc[condition1, 'keywordBid'] / ((data.loc[condition1, 'ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[condition1, 'Action_Reason'] = '根据定义一调整竞价'

# 定义2
condition2 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36)
data.loc[condition2, 'New_keywordBid'] = data.loc[condition2, 'keywordBid'] / ((data.loc[condition2, 'ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[condition2, 'Action_Reason'] = '根据定义二调整竞价'

# 定义3
condition3 = (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36)
data.loc[condition3, 'New_keywordBid'] = data.loc[condition3, 'keywordBid'] - 0.04
data.loc[condition3, 'Action_Reason'] = '根据定义三调整竞价'

# 定义4
condition4 = (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5)
data.loc[condition4, 'New_keywordBid'] = '关闭'
data.loc[condition4, 'Action_Reason'] = '根据定义四关闭'

# 定义5
condition5 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36)
data.loc[condition5, 'New_keywordBid'] = '关闭'
data.loc[condition5, 'Action_Reason'] = '根据定义五关闭'

# 定义6
condition6 = (data['total_sales14d_30d'] == 0) & (data['total_cost_30d'] >= 5)
data.loc[condition6, 'New_keywordBid'] = '关闭'
data.loc[condition6, 'Action_Reason'] = '根据定义六关闭'

# 定义7
condition7 = (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15) & (data['total_clicks_7d'] > 0)
data.loc[condition7, 'New_keywordBid'] = '关闭'
data.loc[condition7, 'Action_Reason'] = '根据定义七关闭'

# 筛选需要的列
result = data[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_7d', 'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', 'Action_Reason']]
# 筛选出被识别为需要处理的商品投放
result = result[result['Action_Reason'] != '']

# 保存结果到CSV
result.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_IT_2024-07-10.csv', index=False)

print('数据处理完成，文件已保存。')