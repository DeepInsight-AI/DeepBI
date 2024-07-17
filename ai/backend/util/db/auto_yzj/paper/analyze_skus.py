# filename: analyze_skus.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
df = pd.read_csv(file_path)

# 筛选满足定义一或定义二的SKU
filtered_df = df[
    (
        (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) &
        (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
    ) |
    (
        (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) &
        (df['total_clicks_7d'] == 0)
    )
]

# 添加满足的定义
filtered_df['满足的定义'] = filtered_df.apply(
    lambda row: (
        '定义一'
        if (row['ACOS_30d'] > 0) & (row['ACOS_30d'] <= 0.24) &
           (row['ACOS_7d'] > 0) & (row['ACOS_7d'] <= 0.24)
        else '定义二'
    ),
    axis=1
)

# 提取需要的列
output_df = filtered_df[[
    'campaignName',
    'adId',
    'adGroupName',
    'ACOS_30d',
    'ACOS_7d',
    'total_clicks_7d',
    'advertisedSku',
    'ORDER_1m',
    '满足的定义'
]]

# 保存结果到指定CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_ES_2024-06-26.csv'
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print('结果已成功保存到指定路径。')