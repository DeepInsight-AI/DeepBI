# filename: filter_sku_corrected.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
data = pd.read_csv(file_path)

# 定义筛选条件的逻辑
conditions = [
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['total_cost_7d'] > 5),
    (data['ORDER_1m'] < 8) & (data['ACOS_30d'] > 0.24) & (data['total_sales_7d'] == 0) & (data['total_cost_7d'] > 5),
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'].between(0.24, 0.5)) & (data['ACOS_30d'].between(0, 0.24)) & (data['total_cost_7d'] > 5),
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24),
    (data['ACOS_7d'] > 0.5),
    (data['total_cost_30d'] > 5) & (data['total_sales_30d'] == 0),
    (data['ORDER_1m'] < 8) & (data['total_cost_7d'] >= 5) & (data['total_sales_7d'] == 0),
    (data['ORDER_1m'] >= 8) & (data['total_cost_7d'] >= 10) & (data['total_sales_7d'] == 0)
]

# 将满足任意一个条件的sku筛选出来
filtered_data = data[pd.concat(conditions, axis=1).any(axis=1)]

# 输出符合条件的结果
output_columns = [
    'campaignName', 'adId', 'adGroupName', 
    'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 
    'advertisedSku', 'ORDER_1m'
]

output_data = filtered_data[output_columns]

# 保存结果到新CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_UK_2024-07-11.csv'
output_data.to_csv(output_file_path, index=False)

print(f'Successfully saved the filtered data to {output_file_path}')