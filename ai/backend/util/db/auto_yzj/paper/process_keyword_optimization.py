# filename: process_keyword_optimization.py

import pandas as pd

# 读取CSV文件
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放搜索词优化/预处理.csv"
data = pd.read_csv(file_path)

# 定义筛选条件
condition_1 = (data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)
condition_2 = (data['ORDER_1m'] > 0) & (data['ACOS_30d'] < 0.24)

# 满足定义一或定义二的搜索词
filtered_data = data[(condition_1) | (condition_2)].copy()

# 添加新的列，用于指示满足的定义
filtered_data['reason'] = ""
filtered_data.loc[condition_1, 'reason'] = "定义一"
filtered_data.loc[condition_2, 'reason'] += "定义二"

# 筛选需要输出的列
output_columns = [
    'campaignName', 'adGroupName', 'ACOS_7d', 'total_sales14d_7d',
    'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]
output_data = filtered_data[output_columns]

# 保存结果到新的CSV文件
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放搜索词优化/提问策略/手动_ASIN_优质搜索词_v1_1_IT_2024-06-27.csv"
output_data.to_csv(output_file_path, index=False)

print("数据处理完成，结果已保存到新CSV文件中。")