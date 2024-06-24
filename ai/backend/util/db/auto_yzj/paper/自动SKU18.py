# filename: find_sku.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\SKU优化\\预处理.csv"
df = pd.read_csv(file_path, encoding='utf-8')

# 筛选符合条件的SKU
def is_condition_1(row):
    return row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['total_clicks_7d'] > 13

def is_condition_2(row):
    return row['ORDER_1m'] < 8 and row['ACOS_30d'] > 0.24 and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 13

def is_condition_3(row):
    return row['ORDER_1m'] < 8 and 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24 and row['total_clicks_7d'] > 13

def is_condition_4(row):
    return row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['ACOS_30d'] > 0.24

def is_condition_5(row):
    return row['ACOS_7d'] > 0.5

def is_condition_6(row):
    return row['total_clicks_30d'] > 13 and row['total_sales14d_30d'] == 0

def is_condition_7(row):
    return row['total_clicks_7d'] >= 19 and row['total_sales14d_7d'] == 0

def get_condition(row):
    if is_condition_1(row):
        return "定义一"
    if is_condition_2(row):
        return "定义二"
    if is_condition_3(row):
        return "定义三"
    if is_condition_4(row):
        return "定义四"
    if is_condition_5(row):
        return "定义五"
    if is_condition_6(row):
        return "定义六"
    if is_condition_7(row):
        return "定义七"
    return None

# 新数据框架, 只包含满足条件的SKU和相关字段
filtered_df = df.apply(get_condition, axis=1).dropna().map(lambda x: {"Condition": x})
target_columns = ["campaignName", "adId", "adGroupName", "ACOS_30d", "ACOS_7d", "total_clicks_7d", "advertisedSku", "ORDER_1m"]
filtered_columns = df[target_columns]

result_df = pd.concat([filtered_columns, pd.DataFrame(filtered_df)], axis=1).dropna()

# 将结果写入新的CSV文件
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\SKU优化\\提问策略\\自动_关闭SKU_v1_1_ES_2024-06-18.csv"
result_df.to_csv(output_path, index=False, encoding='utf-8')

print(f"筛选后的数据已保存到 {output_path}")