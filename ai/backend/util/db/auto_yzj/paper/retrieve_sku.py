# filename: retrieve_sku.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 定义过滤条件
condition_1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
condition_2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 满足条件一或条件二的SKU
filtered_data = data[condition_1 | condition_2]

# 添加定义列
def add_definition(row):
    if (row['ACOS_30d'] > 0) and (row['ACOS_30d'] <= 0.24):
        if (row['ACOS_7d'] > 0) and (row['ACOS_7d'] <= 0.24):
            return '定义一'
        if row['total_clicks_7d'] == 0:
            return '定义二'
    return '其他'

filtered_data['定义'] = filtered_data.apply(add_definition, axis=1)

# 筛选并输出所需列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '定义'
]
output_data = filtered_data[output_columns]

# 将结果保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_LAPASA_FR_2024-07-02.csv'
output_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("数据已成功过滤并保存到CSV文件中。")