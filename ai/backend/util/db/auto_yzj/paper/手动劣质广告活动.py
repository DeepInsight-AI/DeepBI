# filename: budget_optimization.py

import pandas as pd

# 设置文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_v1_1_IT_2024-06-13.csv'

# 读取数据
df = pd.read_csv(file_path)

# 添加新列
df['New_Budget'] = df['Budget']
df['原因'] = ""

# 定义更新预算和原因的函数
def update_budget_and_reason(row, reason, min_budget):
    new_budget = row['Budget'] - 5
    if new_budget < min_budget:
        new_budget = min_budget
    row['New_Budget'] = new_budget
    row['原因'] = reason
    return row

# 为结果数据帧创建空列表
result = []

# 遍历每一行并应用判断条件
for idx, row in df.iterrows():
    if row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and row['clicks_yesterday'] >= 10 and row['ACOS_30d'] > row['country_avg_ACOS_1m']:
        if row['Budget'] > 8:
            df.loc[idx] = update_budget_and_reason(row, '定义一', 8)
        result.append(row)

    elif row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and row['cost_yesterday'] > 0.8 * row['Budget'] and row['ACOS_30d'] > row['country_avg_ACOS_1m']:
        if row['Budget'] > 8:
            df.loc[idx] = update_budget_and_reason(row, "定义二", 8)
        result.append(row)

    elif row['ACOS_30d'] > 0.24 and row['ACOS_30d'] > row['country_avg_ACOS_1m'] and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] >= 15:
        if row['Budget'] > 5:
            df.loc[idx] = update_budget_and_reason(row, "定义三", 5)
        result.append(row)

    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 75:
        row['New_Budget'] = "关闭"
        row['原因'] = "定义四"
        df.loc[idx] = row
        result.append(row)

# 如果result为空，避免创建空的数据帧并保存
if result:
    result_df = pd.DataFrame(result)

    # 选择需要的列
    result_df = result_df[[
        'campaignId',
        'campaignName',
        'Budget',
        'New_Budget',
        'clicks_yesterday',
        'ACOS_yesterday',
        'ACOS_7d',
        'total_clicks_7d',
        'total_sales14d_7d',
        'ACOS_30d',
        'total_clicks_30d',
        'total_sales14d_30d',
        'country_avg_ACOS_1m',
        '原因'
    ]]

    # 保存结果到CSV
    result_df.to_csv(output_file_path, index=False)
    print("Processing complete. Results saved to:", output_file_path)
else:
    print("No records matched the criteria.")