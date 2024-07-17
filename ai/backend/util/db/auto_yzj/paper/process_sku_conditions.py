# filename: process_sku_conditions.py
import pandas as pd

# 读入CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
df = pd.read_csv(file_path)

# 筛选符合定义一或定义二的SKU
condition_one = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
condition_two = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['total_clicks_7d'] == 0)

filtered_df = df[condition_one | condition_two]

# 添加满足条件类型的列
filtered_df['满足的定义'] = filtered_df.apply(lambda row: '定义一' if condition_one.loc[row.name] else '定义二', axis=1)

# 选择所需的列
output_columns = [
    'campaignName',
    'adId',
    'adGroupName',
    'ACOS_30d',
    'ACOS_7d',
    'total_clicks_7d',
    'advertisedSku',
    'ORDER_1m',
    '满足的定义'
]

output_df = filtered_df[output_columns]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_ES_2024-06-30.csv'
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"处理结果已保存到： {output_file_path}")