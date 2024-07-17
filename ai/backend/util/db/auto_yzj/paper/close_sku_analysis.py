# filename: close_sku_analysis.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path)

# 定义条件
conditions = [
    (
        (df['ORDER_1m'] < 5) &
        (df['ACOS_7d'] > 0.6) &
        (df['total_clicks_7d'] > 13),
        '定义一'
    ),
    (
        (df['ORDER_1m'] < 5) &
        (df['ACOS_30d'] > 0.6) &
        (df['total_sales14d_7d'] == 0) &
        (df['total_clicks_7d'] > 13),
        '定义二'
    ),
    (
        (df['ORDER_1m'] < 5) &
        (df['ACOS_7d'] > 0.6) &
        (df['ACOS_30d'] > 0.6),
        '定义三'
    ),
    (
        (df['total_clicks_30d'] > 50) &
        (df['total_sales14d_30d'] == 0),
        '定义四'
    ),
    (
        (df['ORDER_1m'] < 5) &
        (df['total_clicks_7d'] >= 19) &
        (df['total_sales14d_7d'] == 0),
        '定义五'
    ),
    (
        (df['total_clicks_7d'] >= 30) &
        (df['total_sales14d_7d'] == 0),
        '定义六'
    )
]

filtered_df = pd.DataFrame()

# 应用条件
for condition, definition in conditions:
    temp_df = df[condition].copy()
    temp_df['满足的定义'] = definition
    filtered_df = pd.concat([filtered_df, temp_df])

# 筛选所需的列
filtered_df = filtered_df[[
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 
    'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义'
]]

# 去重
filtered_df.drop_duplicates(inplace=True)

# 保存结果
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\关闭SKU\提问策略\自动_关闭SKU_v1_1_LAPASA_DE_2024-07-03.csv'
filtered_df.to_csv(output_path, index=False)
print(f"结果已保存到 {output_path}")