# filename: filter_sku.py

import pandas as pd

# 定义CSV文件路径
csv_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_IT_2024-07-13.csv"

# 读取CSV文件
data = pd.read_csv(csv_file_path)

# 筛选符合条件的SKU
filtered_data_1 = data[
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['total_cost_7d'] > 5)
]

filtered_data_2 = data[
    (data['ORDER_1m'] < 8) & (data['ACOS_30d'] > 0.24) & (data['total_sales_7d'] == 0) & (data['total_cost_7d'] > 5)
]

filtered_data_3 = data[
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'].between(0.24, 0.5)) & (data['ACOS_30d'].between(0, 0.24)) & (data['total_cost_7d'] > 5)
]

filtered_data_4 = data[
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24)
]

filtered_data_5 = data[
    (data['ACOS_7d'] > 0.5)
]

filtered_data_6 = data[
    (data['total_cost_30d'] > 5) & (data['total_sales_30d'] == 0)
]

filtered_data_7 = data[
    (data['ORDER_1m'] < 8) & (data['total_cost_7d'] >= 5) & (data['total_sales_7d'] == 0)
]

filtered_data_8 = data[
    (data['ORDER_1m'] >= 8) & (data['total_cost_7d'] >= 10) & (data['total_sales_7d'] == 0)
]

filtered_data = pd.concat([filtered_data_1, filtered_data_2, filtered_data_3, filtered_data_4, filtered_data_5, filtered_data_6, filtered_data_7, filtered_data_8]).drop_duplicates()

# 生成结果数据集
result_data = filtered_data[[
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 
    'total_clicks_7d', 'advertisedSku', 'ORDER_1m'
]]

# 初始化并添加满足的定义列
result_data['满足的定义'] = ''
result_data.loc[(result_data['ORDER_1m'] < 8) & (result_data['ACOS_7d'] > 0.24) & (result_data['total_cost_7d'] > 5), '满足的定义'] = '定义一'
result_data.loc[(result_data['ORDER_1m'] < 8) & (result_data['ACOS_30d'] > 0.24) & (result_data['total_sales_7d'] == 0) & (result_data['total_cost_7d'] > 5), '满足的定义'] = '定义二'
result_data.loc[(result_data['ORDER_1m'] < 8) & (result_data['ACOS_7d'].between(0.24, 0.5)) & (result_data['ACOS_30d'].between(0, 0.24)) & (result_data['total_cost_7d'] > 5), '满足的定义'] = '定义三'
result_data.loc[(result_data['ORDER_1m'] < 8) & (result_data['ACOS_7d'] > 0.24) & (result_data['ACOS_30d'] > 0.24), '满足的定义'] = '定义四'
result_data.loc[(result_data['ACOS_7d'] > 0.5), '满足的定义'] = '定义五'
result_data.loc[(result_data['total_cost_30d'] > 5) & (result_data['total_sales_30d'] == 0), '满足的定义'] = '定义六'
result_data.loc[(result_data['ORDER_1m'] < 8) & (result_data['total_cost_7d'] >= 5) & (result_data['total_sales_7d'] == 0), '满足的定义'] = '定义七'
result_data.loc[(result_data['ORDER_1m'] >= 8) & (result_data['total_cost_7d'] >= 10) & (result_data['total_sales_7d'] == 0), '满足的定义'] = '定义八'

# 保存结果到CSV文件
result_data.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")