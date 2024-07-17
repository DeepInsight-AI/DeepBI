# filename: SD_关闭SKU_analysis.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path)

# 定义条件
cond1 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_cost_7d'] > 5)
cond2 = (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales_7d'] == 0) & (df['total_cost_7d'] > 5)
cond3 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_cost_7d'] > 5)
cond4 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
cond5 = (df['ACOS_7d'] > 0.5)
cond6 = (df['total_cost_30d'] > 5) & (df['total_sales_30d'] == 0)
cond7 = (df['ORDER_1m'] < 8) & (df['total_cost_7d'] >= 5) & (df['total_sales_7d'] == 0)
cond8 = (df['ORDER_1m'] >= 8) & (df['total_cost_7d'] >= 10) & (df['total_sales_7d'] == 0)

# 筛选数据
filtered_df = df[cond1 | cond2 | cond3 | cond4 | cond5 | cond6 | cond7 | cond8].copy()

# 添加满足的定义列
filtered_df['满足的定义'] = ''
filtered_df.loc[cond1, '满足的定义'] += '定义一;'
filtered_df.loc[cond2, '满足的定义'] += '定义二;'
filtered_df.loc[cond3, '满足的定义'] += '定义三;'
filtered_df.loc[cond4, '满足的定义'] += '定义四;'
filtered_df.loc[cond5, '满足的定义'] += '定义五;'
filtered_df.loc[cond6, '满足的定义'] += '定义六;'
filtered_df.loc[cond7, '满足的定义'] += '定义七;'
filtered_df.loc[cond8, '满足的定义'] += '定义八;'

# 保留所需列
output_df = filtered_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义']]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_US_2024-07-12.csv'
output_df.to_csv(output_file_path, index=False)

print(f"数据已保存到 {output_file_path}")