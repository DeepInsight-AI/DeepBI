# filename: auto_open_sku.py

import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 过滤满足定义一的SKU数据
condition1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24)
condition2 = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
define1 = condition1 & condition2

# 过滤满足定义二的SKU数据
condition3 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24)
condition4 = (data['total_clicks_7d'] == 0)
define2 = condition3 & condition4

# 综合两个定义，满足任一条件的SKU数据都需保留
filtered_data = data[define1 | define2]

# 需要输出的列
output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m']

# 将过滤后的数据输出为新的CSV文件
output_data = filtered_data[output_columns]
output_data['define_met'] = ['定义一' if d1 else '定义二' for d1 in define1[define1 | define2]]

# 指定保存路径
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_LAPASA_US_2024-07-04.csv'
output_data.to_csv(output_file_path, index=False)

print("数据已成功过滤并保存到CSV文件中。")