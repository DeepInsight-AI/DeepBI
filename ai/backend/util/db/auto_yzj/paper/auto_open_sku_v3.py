# filename: auto_open_sku_v3.py

import pandas as pd

# 读取数据
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_IT_2024-06-26.csv'

df = pd.read_csv(input_file)

# 筛选符合条件的数据
# 定义一
condition1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
# 定义二
condition2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['total_clicks_7d'] == 0)

# 满足定义一或定义二
filter_conditions = condition1 | condition2

filtered_df = df[filter_conditions]

# 添加“满足的定义”列
filtered_df['满足的定义'] = ['定义一' if (row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.24 and row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.24) else '定义二' for _, row in filtered_df.iterrows()]

# 选取所需列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义'
]
output_df = filtered_df[output_columns]

# 保存结果到CSV文件
output_df.to_csv(output_file, index=False)

print(f"结果已保存到 {output_file}")