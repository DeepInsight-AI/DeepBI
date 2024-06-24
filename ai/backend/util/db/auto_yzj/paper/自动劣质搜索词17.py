# filename: search_term_analysis.py

import pandas as pd

# 读入数据
data_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv"
data = pd.read_csv(data_path)

# 筛选符合条件的数据
results = []

# 定义一
condition1 = (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.36) & (data['total_sales14d_30d'] <= 5)
results.append(data[condition1].assign(reason="定义一"))

# 定义二
condition2 = (data['ACOS_30d'] >= 0.36) & (data['total_sales14d_30d'] <= 8)
results.append(data[condition2].assign(reason="定义二"))

# 定义三
condition3 = (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)
results.append(data[condition3].assign(reason="定义三"))

# 定义四
condition4 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.36) & (data['total_sales14d_7d'] <= 3)
results.append(data[condition4].assign(reason="定义四"))

# 定义五
condition5 = (data['ACOS_7d'] >= 0.36) & (data['total_sales14d_7d'] <= 5)
results.append(data[condition5].assign(reason="定义五"))

# 定义六
condition6 = (data['total_clicks_7d'] > 10) & (data['total_sales14d_7d'] == 0)
results.append(data[condition6].assign(reason="定义六"))

# 合并所有符合条件的结果
final_result = pd.concat(results).drop_duplicates()

# 选择需要的字段
final_result = final_result[[
    'campaignName', 
    'campaignId', 
    'adGroupName', 
    'adGroupId', 
    'total_clicks_7d',
    'ACOS_7d', 
    'total_sales14d_7d', 
    'total_clicks_30d',
    'total_sales14d_30d',
    'ACOS_30d',
    'searchTerm', 
    'reason'
]]

# 保存结果到指定路径
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_劣质搜索词_v1_1_IT_2024-06-17.csv"
final_result.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"结果保存到: {output_path}")