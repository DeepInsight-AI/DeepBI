# filename: budget_optimization.py

import pandas as pd

# 读取CSV文件
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/预算优化/预处理.csv"
df = pd.read_csv(file_path)

# 根据逻辑判断表现较差的广告活动并调整预算
def adjust_budget(row):
    reasons = []
    new_budget = row['Budget']

    # 定义一条件
    if row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and row['clicks_yesterday'] >= 10 and row['ACOS_30d'] > row['country_avg_ACOS_1m']:
        reasons.append('定义一')
        new_budget = max(8, new_budget - 5)
    
    # 定义二条件
    if row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and row['cost_yesterday'] > 0.8 * row['Budget'] and row['ACOS_30d'] > row['country_avg_ACOS_1m']:
        reasons.append('定义二')
        new_budget = max(8, new_budget - 5)
    
    # 定义三条件
    if row['ACOS_30d'] > 0.24 and row['ACOS_30d'] > row['country_avg_ACOS_1m'] and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] >= 15:
        reasons.append('定义三')
        new_budget = max(5, new_budget - 5)

    # 定义四条件
    if row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 75:
        reasons.append('定义四')
        new_budget = "关闭"
    
    return pd.Series({"New_Budget": new_budget, "Reason": ",".join(reasons)})

# 应用调整函数
df[['New_Budget', 'Reason']] = df.apply(adjust_budget, axis=1)

# 过滤出需要调整预算或关闭的广告活动
result_df = df[(df['New_Budget'] != df['Budget']) | (df['Reason'] != '')]

# 保存结果到新的CSV文件中
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/预算优化/提问策略/自动_劣质广告活动_v1_1_IT_2024-06-17.csv"
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_file_path}")