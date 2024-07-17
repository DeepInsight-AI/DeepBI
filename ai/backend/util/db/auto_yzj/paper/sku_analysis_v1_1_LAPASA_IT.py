# filename: sku_analysis_v1_1_LAPASA_IT.py

import pandas as pd

# 数据文件路径
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_IT_2024-07-03.csv'

# 读取csv文件
df = pd.read_csv(input_file_path)

# 定义一 (Definition 1)
condition1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)

# 定义二 (Definition 2)
condition2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['total_clicks_7d'] == 0)

# 满足定义一或定义二 (满足任意一个条件)
result = df[condition1 | condition2]

# 添加满足条件定义的标识列
result['满足的定义'] = '定义一或定义二'

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
    '满足的定义'
]

# 输出结果到新的CSV文件
result.to_csv(output_file_path, columns=output_columns, index=False)

print(f'结果已保存到: {output_file_path}')