# filename: 自动_复开SKU_v1_1_ES_2024-06-26.py

import pandas as pd

# 读取CSV文件路径
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv"
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_ES_2024-06-26.csv"

# 读取数据集
df = pd.read_csv(file_path)

# 定义一筛选
condition1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & \
             (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)

# 定义二筛选
condition2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & \
             (df['total_clicks_7d'] == 0)

# 过滤数据
filtered_df1 = df[condition1].copy()
filtered_df1['matched_definition'] = '定义一'

filtered_df2 = df[condition2].copy()
filtered_df2['matched_definition'] = '定义二'

# 合并并去重
result_df = pd.concat([filtered_df1, filtered_df2]).drop_duplicates()

# 选择需要输出的列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d',
    'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'matched_definition'
]

# 输出到CSV文件
result_df[output_columns].to_csv(output_path, index=False, encoding='utf-8-sig')

print("筛选结果已保存至:", output_path)