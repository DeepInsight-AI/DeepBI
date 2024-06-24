# filename: sku_filter.py

import pandas as pd

# 加载CSV文件
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/SKU优化/预处理.csv'
data = pd.read_csv(file_path)

# 定义筛选条件
conditions = [
    (data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24),  # 定义一
    (data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10),  # 定义二
    (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_clicks_7d'] > 13),  # 定义三
    (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24),  # 定义四
    (data['ACOS_7d'] > 0.5),  # 定义五
    (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)  # 定义六
]

# 聚合所有条件
combined_condition = conditions[0]
for condition in conditions[1:]:
    combined_condition |= condition

# 应用筛选条件
filtered_data = data[combined_condition]

# 选择输出的列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku'
]
output_data = filtered_data[output_columns]

# 保存结果到CSV文件
output_file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/SKU优化/提问策略/自动_关闭SKU_v1_1_IT_2024-06-17.csv'
output_data.to_csv(output_file_path, index=False)

print(f"筛选后的数据已保存到 {output_file_path}")