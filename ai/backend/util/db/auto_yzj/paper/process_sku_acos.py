# filename: process_sku_acos.py

import pandas as pd

# 文件路径
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_DE_2024-06-26.csv'

# 读取数据
df = pd.read_csv(input_file_path)

# 筛选符合定义一的SKU
condition1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24)
condition2 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
defined1_sku = df[condition1 & condition2]

# 筛选符合定义二的SKU
condition3 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24)
condition4 = df['total_clicks_7d'] == 0
defined2_sku = df[condition3 & condition4]

# 合并两个定义的SKU
result_df = pd.concat([defined1_sku, defined2_sku]).drop_duplicates()

# 添加满足的定义列
result_df['满足的定义'] = None

result_df.loc[((result_df['ACOS_30d'] > 0) & (result_df['ACOS_30d'] <= 0.24) & (result_df['ACOS_7d'] > 0) & (result_df['ACOS_7d'] <= 0.24)), '满足的定义'] = '定义一'
result_df.loc[((result_df['ACOS_30d'] > 0) & (result_df['ACOS_30d'] <= 0.24) & (result_df['total_clicks_7d'] == 0)), '满足的定义'] = '定义二'

# 选择需要的列
output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义']
final_result = result_df[output_columns]

# 将结果保存到CSV文件
final_result.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"筛选结果已保存到 {output_file_path}")