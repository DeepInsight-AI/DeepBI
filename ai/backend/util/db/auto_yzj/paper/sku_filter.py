# filename: sku_filter.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
data = pd.read_csv(file_path)

# 定义条件
conditions = [
    ((data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['total_cost_7d'] > 5)),
    ((data['ORDER_1m'] < 8) & (data['ACOS_30d'] > 0.24) & (data['total_sales_7d'] == 0) & (data['total_cost_7d'] > 5)),
    ((data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_cost_7d'] > 5)),
    ((data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24)),
    (data['ACOS_7d'] > 0.5),
    ((data['total_cost_30d'] > 5) & (data['total_sales_30d'] == 0)),
    ((data['ORDER_1m'] < 8) & (data['total_cost_7d'] >= 5) & (data['total_sales_7d'] == 0)),
    ((data['ORDER_1m'] >= 8) & (data['total_cost_7d'] >= 10) & (data['total_sales_7d'] == 0))
]

# 添加标识列
data['definition'] = ''

for i, condition in enumerate(conditions, 1):
    data.loc[condition, 'definition'] += f'定义{i};'

# 筛选出符合条件的行
filtered_data = data[data['definition'] != '']

# 排出需要的列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d',
    'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'definition'
]
output_data = filtered_data[output_columns]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_ES_2024-07-13.csv'
output_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("Result saved to:", output_file_path)