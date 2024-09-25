# filename: C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\关闭SKU\\提问策略\\filter_sku.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\关闭SKU\\预处理.csv'
df = pd.read_csv(file_path)

# 定义条件1到条件8
conditions = [
    (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_cost_7d'] > 5),
    (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales_7d'] == 0) & (df['total_cost_7d'] > 5),
    (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_cost_7d'] > 5),
    (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24),
    (df['ACOS_7d'] > 0.5),
    (df['total_cost_30d'] > 5) & (df['total_sales_30d'] == 0),
    (df['ORDER_1m'] < 8) & (df['total_cost_7d'] >= 5) & (df['total_sales_7d'] == 0),
    (df['ORDER_1m'] >= 8) & (df['total_cost_7d'] >= 10) & (df['total_sales_7d'] == 0)
]

# 创建条件名称
condition_names = [
    '定义一', '定义二', '定义三', '定义四', '定义五', '定义六', '定义七', '定义八'
]

# 初始化一个空的DataFrame来存储结果
result_df = pd.DataFrame()

for i, condition in enumerate(conditions):
    # 筛选符合条件的行
    filtered_df = df[condition].copy()
    if not filtered_df.empty:
        filtered_df['满足的定义'] = condition_names[i]
        result_df = result_df.append(filtered_df)

# 只保留需要的列
result_df = result_df[[
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义'
]]

# 保存结果为新的CSV文件
output_file_path = r'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\关闭SKU\\提问策略\\SD_关闭SKU_v1_1_LAPASA_ES_2024-07-18.csv'
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_file_path}")