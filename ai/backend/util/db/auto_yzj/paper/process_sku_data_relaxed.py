# filename: process_sku_data_relaxed.py

import pandas as pd

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_IT_2024-07-16.csv'

# 读取数据
df = pd.read_csv(input_file)

# 定义满足条件的DataFrame列表
result_df_list = []

# 各种定义的条件
conditions = [
    (
        (df['ORDER_1m'] < 5) &
        (df['ACOS_7d'] > 0.01) &
        (df['total_cost_7d'] > 0.1),
        '定义一'
    ),
    (
        (df['ORDER_1m'] < 5) &
        (df['ACOS_30d'] > 0.01) &
        (df['total_sales_7d'] == 0) &
        (df['total_cost_7d'] > 0.1),
        '定义二'
    ),
    (
        (df['ORDER_1m'] < 5) &
        (df['ACOS_7d'] > 0.01) & (df['ACOS_7d'] < 0.1) &
        (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.01) &
        (df['total_cost_7d'] > 0.1),
        '定义三'
    ),
    (
        (df['ORDER_1m'] < 5) &
        (df['ACOS_7d'] > 0.01) &
        (df['ACOS_30d'] > 0.01),
        '定义四'
    ),
    (
        (df['ACOS_7d'] > 0.1),
        '定义五'
    ),
    (
        (df['total_cost_30d'] > 0.1) &
        (df['total_sales_30d'] == 0),
        '定义六'
    ),
    (
        (df['ORDER_1m'] < 5) &
        (df['total_cost_7d'] >= 0.1) &
        (df['total_sales_7d'] == 0),
        '定义七'
    ),
    (
        (df['ORDER_1m'] >= 5) &
        (df['total_cost_7d'] >= 0.2) &
        (df['total_sales_7d'] == 0),
        '定义八'
    )
]

# 应用条件并记录定义
for condition, definition in conditions:
    matched_df = df[condition].copy()
    if not matched_df.empty:
        matched_df['definition'] = definition
        result_df_list.append(matched_df)

# 合并所有符合条件的DataFrame
if result_df_list:
    result_df = pd.concat(result_df_list).drop_duplicates()

    # 选择所需列并保存结果
    result_df = result_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'definition']]
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"筛选后的数据已保存至: {output_file}")
else:
    print("没有符合条件的数据。")