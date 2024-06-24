# filename: process_sku_data.py

import pandas as pd

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_v1_1_ES_2024-06-14.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 筛选条件
condition1 = (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)
condition2 = (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)
condition3 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)
condition4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
condition5 = df['ACOS_7d'] > 0.5
condition6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)

# 选择满足任意一个条件的数据
filtered_df = df[condition1 | condition2 | condition3 | condition4 | condition5 | condition6]

# 输出指定列
output_df = filtered_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku']]
output_df.to_csv(output_file, index=False)
print(f"Filtered data has been saved to {output_file}")