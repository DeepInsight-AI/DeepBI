# filename: extract_sku.py

import pandas as pd

# 文件路径定义
input_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv'
output_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\手动_关闭SKU_v1_1_ES_2024-06-20.csv'

# 读取CSV数据
df = pd.read_csv(input_csv_path)

# 定义筛选条件
condition1 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_clicks_7d'] > 13)
condition2 = (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 13)
condition3 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'].between(0.24, 0.5)) & (df['ACOS_30d'].between(0, 0.24)) & (df['total_clicks_7d'] > 13)
condition4 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
condition5 = df['ACOS_7d'] > 0.5
condition6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)
condition7 = (df['ORDER_1m'] < 8) & (df['total_clicks_7d'] >= 19) & (df['total_sales14d_7d'] == 0)
condition8 = (df['ORDER_1m'] >= 8) & (df['total_clicks_7d'] >= 26) & (df['total_sales14d_7d'] == 0)

# 筛选满足任意一个定义条件的sku
filtered_df = df[condition1 | condition2 | condition3 | condition4 | condition5 | condition6 | condition7 | condition8]

# 添加满足的定义列
filtered_df['match_definition'] = ''
filtered_df.loc[condition1, 'match_definition'] += '定义一,'
filtered_df.loc[condition2, 'match_definition'] += '定义二,'
filtered_df.loc[condition3, 'match_definition'] += '定义三,'
filtered_df.loc[condition4, 'match_definition'] += '定义四,'
filtered_df.loc[condition5, 'match_definition'] += '定义五,'
filtered_df.loc[condition6, 'match_definition'] += '定义六,'
filtered_df.loc[condition7, 'match_definition'] += '定义七,'
filtered_df.loc[condition8, 'match_definition'] += '定义八,'
filtered_df['match_definition'] = filtered_df['match_definition'].str.rstrip(',')

# 选择指定列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 
    'advertisedSku', 'ORDER_1m', 'match_definition'
]
result_df = filtered_df[output_columns]

# 保存结果到CSV文件
result_df.to_csv(output_csv_path, index=False)

print(f"Filtered data has been saved to {output_csv_path}")