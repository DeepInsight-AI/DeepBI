# filename: sku_analysis.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\复开SKU\预处理.csv'
df = pd.read_csv(file_path)

# 筛选满足条件的数据
criteria1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.27) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.27)
criteria2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.27) & (df['total_clicks_7d'] == 0)

filtered_df_definition1 = df[criteria1].copy()
filtered_df_definition1['definition_met'] = 'Definition 1'

filtered_df_definition2 = df[criteria2].copy()
filtered_df_definition2['definition_met'] = 'Definition 2'

# 合并满足条件的数据
final_df = pd.concat([filtered_df_definition1, filtered_df_definition2])

# 筛选需要的字段
output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'definition_met']
final_output = final_df[output_columns]

# 保存结果到新的csv文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_LAPASA_DE_2024-07-03.csv'
final_output.to_csv(output_file_path, index=False)

print(f"输出文件已保存至：{output_file_path}")