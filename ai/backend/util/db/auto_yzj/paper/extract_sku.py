# filename: extract_sku.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 筛选满足定义一条件的SKU
condition1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)

# 筛选满足定义二条件的SKU
condition2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 合并条件一和条件二
filtered_data = data[condition1 | condition2]

# 新增一列来标记满足的定义
filtered_data['满足的定义'] = '定义一'
filtered_data.loc[(data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0), '满足的定义'] = '定义二'

# 选择需要导出的列
export_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义'
]

# 导出结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_US_2024-07-02.csv'
filtered_data.to_csv(output_path, columns=export_columns, index=False)

print(f"Filtered data has been saved to {output_path}")