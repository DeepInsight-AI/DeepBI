# filename: budget_optimization_adjusted.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 调整后的条件定义
condition1_part2_adjusted = df['ACOS'] > 0.22
condition2_part2_adjusted = df['ACOS'] > 0.22

# 满足定义一的广告活动 (调整ACOS条件)
condition1_adjusted = (
    (df['avg_ACOS_7d'] > 0.24) &
    (condition1_part2_adjusted) &
    (df['clicks'] >= 10) &
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
)

# 满足定义二的广告活动 (调整ACOS条件)
condition2_adjusted = (
    (df['avg_ACOS_7d'] > 0.24) &
    (condition2_part2_adjusted) &
    (df['cost'] > df['Budget'] * 0.8) &
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
)

# 满足定义三的广告活动
condition3 = (
    (df['avg_ACOS_1m'] > 0.24) &
    (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']) &
    (df['sales_1m'] == 0) &
    (df['clicks_7d'] >= 15)
)

# 根据条件计算新预算
def calculate_new_budget(row, condition1, condition2, condition3):
    if condition1.loc[row.name] or condition2.loc[row.name]:
        new_budget = max(row['Budget'] - 5, 8)
    elif condition3.loc[row.name]:
        new_budget = max(row['Budget'] - 5, 5)
    else:
        new_budget = row['Budget']
    return new_budget

# 计算新预算
df['new_Budget'] = df.apply(lambda row: calculate_new_budget(row, condition1_adjusted, condition2_adjusted, condition3), axis=1)

# 过滤出满足条件的广告活动并增加原因列
inferior_campaigns_adjusted = df[condition1_adjusted | condition2_adjusted | condition3].copy()
inferior_campaigns_adjusted['reason'] = ''

inferior_campaigns_adjusted.loc[condition1_adjusted, 'reason'] = '定义一 (调整后)'
inferior_campaigns_adjusted.loc[condition2_adjusted, 'reason'] = '定义二 (调整后)'
inferior_campaigns_adjusted.loc[condition3, 'reason'] = '定义三'

# 选择需要输出的列
output_columns = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 
    'clicks_7d', 'sales_1m', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 
    'country_avg_ACOS_1m', 'new_Budget', 'reason'
]

output_df_adjusted = inferior_campaigns_adjusted[output_columns]

# 保存到新的CSV文件
output_file_adjusted = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_劣质广告活动_ES_调整后_2024-06-121.csv'
output_df_adjusted.to_csv(output_file_adjusted, index=False)

print(f"处理完成，结果已保存到 {output_file_adjusted}")