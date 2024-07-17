# filename: script_to_find_sku.py

import pandas as pd

# 读取CSV文件路径及保存路径
input_csv_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv"
output_csv_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_DE_2024-07-03.csv"

# 读取CSV数据
df = pd.read_csv(input_csv_path)

# 筛选满足定义一和定义二的SKU
condition_1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
condition_2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['total_clicks_7d'] == 0)

filtered_df = df[condition_1 | condition_2]

# 添加满足条件的描述列
filtered_df['condition'] = '定义一'
filtered_df.loc[condition_2, 'condition'] = '定义二'

# 选择需要的列
output_columns = [
    'campaignName',
    'adId',
    'adGroupName',
    'ACOS_30d',
    'ACOS_7d',
    'total_clicks_7d',
    'advertisedSku',
    'ORDER_1m',
    'condition'
]

# 保存到新的CSV文件
filtered_df.to_csv(output_csv_path, columns=output_columns, index=False)

print(f"结果已保存到 {output_csv_path}")