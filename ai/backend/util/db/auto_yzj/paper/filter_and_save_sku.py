# filename: filter_and_save_sku.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 筛选满足定义一的SKU
definition_one = data[
    (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) &
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
].copy()
definition_one['满足的定义'] = '定义一'

# 筛选满足定义二的SKU
definition_two = data[
    (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) &
    (data['total_clicks_7d'] == 0)
].copy()
definition_two['满足的定义'] = '定义二'

# 合并定义一和定义二的SKU
result = pd.concat([definition_one, definition_two])

# 选择需要的列
result = result[[
    'campaignName', 'adId', 'adGroupName',
    'ACOS_30d', 'ACOS_7d', 'total_clicks_7d',
    'advertisedSku', 'ORDER_1m', '满足的定义'
]]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_ES_2024-06-28.csv'
result.to_csv(output_file_path, index=False)

print(f"符合条件的SKU已保存到 {output_file_path}")