# filename: find_sku_conditions.py
import pandas as pd

# 指定文件路径
input_filepath = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\滞销品优化\\手动sp广告\\关闭SKU\\预处理.csv'
output_filepath = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\滞销品优化\\手动sp广告\\关闭SKU\\提问策略\\手动_关闭SKU_v1_1_LAPASA_DE_2024-07-03.csv'

# 读取CSV数据
df = pd.read_csv(input_filepath)

# 定义条件
def evaluate_conditions(row):
    conditions_met = []

    # 定义一
    if row['ORDER_1m'] < 5 and row['ACOS_7d'] > 0.6 and row['total_clicks_7d'] > 13:
        conditions_met.append("定义一")

    # 定义二
    if row['ORDER_1m'] < 5 and row['ACOS_30d'] > 0.6 and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 13:
        conditions_met.append("定义二")

    # 定义三
    if row['ORDER_1m'] < 5 and row['ACOS_7d'] > 0.6 and row['ACOS_30d'] > 0.6:
        conditions_met.append("定义三")

    # 定义四
    if row['total_clicks_30d'] > 50 and row['total_sales14d_30d'] == 0:
        conditions_met.append("定义四")

    # 定义五
    if row['ORDER_1m'] < 5 and row['total_clicks_7d'] >= 19 and row['total_sales14d_7d'] == 0:
        conditions_met.append("定义五")

    # 定义六
    if row['total_clicks_7d'] >= 30 and row['total_sales14d_yesterday'] == 0:
        conditions_met.append("定义六")

    return conditions_met

# 应用条件
df['满足的定义'] = df.apply(evaluate_conditions, axis=1)

# 保留满足任意定义的行
result_df = df[df['满足的定义'].apply(len) > 0]

# 选择必要的列进行输出
result_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义']
result_df = result_df[result_columns]

# 保存到新的CSV文件
result_df.to_csv(output_filepath, index=False, encoding='utf-8-sig')

print(f"数据已保存到 {output_filepath}")