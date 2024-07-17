# filename: handle_skus.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\复开SKU\预处理.csv'
df = pd.read_csv(file_path, encoding='utf-8')

# 定义条件
condition1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.27) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.27)
condition2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.27) & (df['total_clicks_7d'] == 0)

# 过滤数据
filtered_df1 = df[condition1].copy()
filtered_df1['满足的定义'] = '定义一'
filtered_df2 = df[condition2].copy()
filtered_df2['满足的定义'] = '定义二'

# 合并数据
filtered_df = pd.concat([filtered_df1, filtered_df2])

# 选择需要的列
result_df = filtered_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义']]

# 输出结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_FR_2024-07-03.csv'
result_df.to_csv(output_path, index=False, encoding='utf-8')

print('CSV file has been saved successfully at:', output_path)