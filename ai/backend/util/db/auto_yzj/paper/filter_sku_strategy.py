# filename: filter_sku_strategy.py

import pandas as pd

# 路径变量
input_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv"
output_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_DE_2024-07-02.csv"

# 加载数据
data = pd.read_csv(input_file)

# 条件一：ACOS_30d > 0 且 ACOS_30d <= 0.24 并且 ACOS_7d > 0 且 ACOS_7d <= 0.24
condition1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)

# 条件二：ACOS_30d > 0 且 ACOS_30d <= 0.24 并且 total_clicks_7d == 0
condition2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 筛选数据
filtered_data = data[condition1 | condition2].copy()

# 添加 "满足的定义" 列
filtered_data['满足的定义'] = ''
filtered_data.loc[condition1, '满足的定义'] = '定义一'
filtered_data.loc[condition2, '满足的定义'] = '定义二'

# 选择需要的列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 
    'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义'
]

# 输出结果
filtered_data.to_csv(output_file, columns=output_columns, index=False)

print(f"Filtered data has been saved to {output_file}")