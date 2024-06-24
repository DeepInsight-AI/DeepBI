# filename: search_term_filter.py

import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合条件的搜索词
filtered_data = pd.DataFrame()

# 定义一
condition1 = (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.36) & (data['total_sales14d_30d'] <= 5)
temp1 = data[condition1].copy()
temp1['reason'] = '定义一'
filtered_data = pd.concat([filtered_data, temp1])

# 定义二
condition2 = (data['ACOS_30d'] >= 0.36) & (data['total_sales14d_30d'] <= 8)
temp2 = data[condition2].copy()
temp2['reason'] = '定义二'
filtered_data = pd.concat([filtered_data, temp2])

# 定义三
condition3 = (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)
temp3 = data[condition3].copy()
temp3['reason'] = '定义三'
filtered_data = pd.concat([filtered_data, temp3])

# 定义四
condition4 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.36) & (data['total_sales14d_7d'] <= 3)
temp4 = data[condition4].copy()
temp4['reason'] = '定义四'
filtered_data = pd.concat([filtered_data, temp4])

# 定义五
condition5 = (data['ACOS_7d'] >= 0.36) & (data['total_sales14d_7d'] <= 5)
temp5 = data[condition5].copy()
temp5['reason'] = '定义五'
filtered_data = pd.concat([filtered_data, temp5])

# 定义六
condition6 = (data['total_clicks_7d'] > 10) & (data['total_sales14d_7d'] == 0)
temp6 = data[condition6].copy()
temp6['reason'] = '定义六'
filtered_data = pd.concat([filtered_data, temp6])

# 选择需要的列
filtered_data = filtered_data[['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'total_clicks_7d', 'ACOS_7d', 'total_sales14d_7d', 'total_clicks_30d', 'total_sales14d_30d', 'ACOS_30d', 'searchTerm', 'reason']]

# 保存到指定路径
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_劣质搜索词_v1_1_ES_2024-06-14.csv'
filtered_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print("过滤后的数据已保存到指定路径。")