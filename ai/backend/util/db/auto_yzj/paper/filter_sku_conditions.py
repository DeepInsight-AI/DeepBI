# filename: filter_sku_conditions.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
data = pd.read_csv(file_path)

# 过滤数据
filtered_data = data[
    ((data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['total_cost_7d'] > 5)) |
    ((data['ORDER_1m'] < 8) & (data['ACOS_30d'] > 0.24) & (data['total_sales_7d'] == 0) & (data['total_cost_7d'] > 5)) |
    ((data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_cost_7d'] > 5)) |
    ((data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24)) |
    (data['ACOS_7d'] > 0.5) |
    ((data['total_cost_30d'] > 5) & (data['total_sales_30d'] == 0)) |
    ((data['ORDER_1m'] < 8) & (data['total_cost_7d'] >= 5) & (data['total_sales_7d'] == 0)) |
    ((data['ORDER_1m'] >= 8) & (data['total_cost_7d'] >= 10) & (data['total_sales_7d'] == 0))
]

# 添加满足的定义列
filtered_data['满足的定义'] = ""

for index, row in filtered_data.iterrows():
    if (row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['total_cost_7d'] > 5):
        filtered_data.at[index, '满足的定义'] += "定义一;"
    if (row['ORDER_1m'] < 8 and row['ACOS_30d'] > 0.24 and row['total_sales_7d'] == 0 and row['total_cost_7d'] > 5):
        filtered_data.at[index, '满足的定义'] += "定义二;"
    if (row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['ACOS_7d'] < 0.5 and row['ACOS_30d'] > 0 and row['ACOS_30d'] < 0.24 and row['total_cost_7d'] > 5):
        filtered_data.at[index, '满足的定义'] += "定义三;"
    if (row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['ACOS_30d'] > 0.24):
        filtered_data.at[index, '满足的定义'] += "定义四;"
    if (row['ACOS_7d'] > 0.5):
        filtered_data.at[index, '满足的定义'] += "定义五;"
    if (row['total_cost_30d'] > 5 and row['total_sales_30d'] == 0):
        filtered_data.at[index, '满足的定义'] += "定义六;"
    if (row['ORDER_1m'] < 8 and row['total_cost_7d'] >= 5 and row['total_sales_7d'] == 0):
        filtered_data.at[index, '满足的定义'] += "定义七;"
    if (row['ORDER_1m'] >= 8 and row['total_cost_7d'] >= 10 and row['total_sales_7d'] == 0):
        filtered_data.at[index, '满足的定义'] += "定义八;"

# 选择所需的列
output_columns = [
    'campaignName',
    'adId',
    'adGroupName',
    'ACOS_30d',
    'ACOS_7d',
    'total_clicks_7d',
    'advertisedSku',
    'ORDER_1m',
    '满足的定义'
]
result_data = filtered_data[output_columns]

# 保存结果
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_UK_2024-07-16.csv'
result_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"结果保存在 {output_path}")