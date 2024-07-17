# filename: auto_reopen_sku.py

import pandas as pd

# 读取CSV文件
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_IT_2024-06-30.csv'

# 读取CSV文件中的数据
data = pd.read_csv(input_file_path)

# 定义筛选规则
condition_1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
condition_2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 综合两个条件
filtered_data = data[condition_1 | condition_2].copy()

# 添加一个新列，用于标记满足的条件
filtered_data.loc[condition_1, '满足的定义'] = '定义一'
filtered_data.loc[condition_2, '满足的定义'] = '定义二'

# 选择感兴趣的列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 
    'advertisedSku', 'ORDER_1m', '满足的定义'
]

output_data = filtered_data[output_columns]

# 将结果保存到新的CSV文件中
output_data.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")