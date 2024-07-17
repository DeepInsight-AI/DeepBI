# filename: process_ads_budget.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选符合条件的广告活动
filtered_df = df[
    (df['ACOS_7d'] < 0.24) &
    (df['ACOS_yesterday'] < 0.24) &
    (df['cost_yesterday'] > 0.8 * df['Budget'])
]

# 调整预算，增加原来预算的1/5，直到预算为50
def adjust_budget(row):
    new_budget = row['Budget'] * 1.2
    if new_budget > 50:
        new_budget = 50
    return new_budget

filtered_df['New Budget'] = filtered_df.apply(adjust_budget, axis=1)

# 添加原因列
filtered_df['Reason'] = 'Good performance: ACOS_7d < 0.24, ACOS_yesterday < 0.24, cost_yesterday > 0.8 * Budget'

# 创建输出数据
output_df = filtered_df[[
    'campaignName', 'New Budget', 'cost_yesterday', 'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d',
    'country_avg_ACOS_1m', 'total_clicks_30d', 'total_sales14d_30d'
]].copy()
output_df['date'] = '2024-05-28'
output_df.rename(columns={
    'cost_yesterday': 'cost',
    'clicks_yesterday': 'clicks',
    'ACOS_yesterday': 'ACOS',
    'New Budget': 'Budget'
}, inplace=True)
output_df = output_df[[
    'date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS', 'ACOS_7d',
    'country_avg_ACOS_1m', 'total_clicks_30d', 'total_sales14d_30d', 'Reason'
]]

# 保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_v1_0_LAPASA_IT_2024-07-09.csv'
output_df.to_csv(output_file_path, index=False)

print("Process completed and results are saved.")