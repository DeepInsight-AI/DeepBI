# filename: batch_reopen_SKU.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
df = pd.read_csv(file_path)

# 过滤满足条件的定义一
condition_1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24)
condition_2 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
df_def_1 = df[condition_1 & condition_2]

# 过滤满足条件的定义二
condition_3 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24)
condition_4 = df['total_clicks_7d'] == 0
df_def_2 = df[condition_3 & condition_4]

# 合并满足定义一和定义二的数据
result_df = pd.concat([df_def_1, df_def_2]).drop_duplicates()

# 添加新的 '定义' 列，标识满足哪种分类
def identify_definition(row):
    if (row['ACOS_30d'] <= 0.24 and row['ACOS_30d'] > 0 and row['ACOS_7d'] <= 0.24 and row['ACOS_7d'] > 0):
        return '定义一'
    elif (row['ACOS_30d'] <= 0.24 and row['ACOS_30d'] > 0 and row['total_clicks_7d'] == 0):
        return '定义二'
    return '不满足'

result_df['定义'] = result_df.apply(identify_definition, axis=1)

# 选择所需的列
output_df = result_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '定义']]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_IT_2024-07-02.csv'
output_df.to_csv(output_file_path, index=False)
print(f"处理完成，结果已保存到 {output_file_path}")