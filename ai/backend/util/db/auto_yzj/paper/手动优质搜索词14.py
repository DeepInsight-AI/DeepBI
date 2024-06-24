# filename: filter_and_output.py

import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv')

# 筛选满足定义一的搜索词
def_one_data = data[(data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)]

# 筛选满足定义二的搜索词
def_two_data = data[(data['total_sales14d_30d'] >= 2) & (data['ACOS_30d'] < 0.24)]

# 合并满足任一定义的搜索词
result_data = pd.concat([def_one_data, def_two_data])

# 输出结果到CSV文件
result_data[['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'ACOS_7d', 'total_sales14d_7d', 'total_sales14d_30d', 'ACOS_30d', 'searchTerm']].to_csv(
    r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_v1_1_ES_2024-06-14_deepseek.csv',
    index=False,
    columns=['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'ACOS_7d', 'total_sales14d_7d', 'total_sales14d_30d', 'ACOS_30d', 'searchTerm'])

# 添加reason列
result_data['reason'] = result_data.apply(lambda row: '定义一' if (row['total_sales14d_7d'] > 0 and row['ACOS_7d'] < 0.2) else '定义二', axis=1)

# 再次输出结果到CSV文件
result_data[['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'ACOS_7d', 'total_sales14d_7d', 'total_sales14d_30d', 'ACOS_30d', 'searchTerm', 'reason']].to_csv(
    r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_v1_1_ES_2024-06-14_deepseek.csv',
    index=False)