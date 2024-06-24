# filename: optimize_budget.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 过滤符合条件的广告活动
filtered_df = df[
    (df['ACOS_7d'] < 0.24) & 
    (df['ACOS_yesterday'] < 0.24) &
    (df['total_cost_7d'] > 0.8 * df['total_cost_7d'] / 7)
]

# 计算新的预算
filtered_df['New_Budget'] = filtered_df['total_cost_7d'] * 1.2
filtered_df.loc[filtered_df['New_Budget'] > 50, 'New_Budget'] = 50

# 添加原因列
filtered_df['Reason'] = "ACOS_7d < 0.24, ACOS_yesterday < 0.24, cost > 80% of budget"

# 输出需要的列
output_df = filtered_df[[
    'campaignId', 
    'campaignName', 
    'total_cost_7d', 
    'New_Budget', 
    'total_cost_7d', 
    'total_clicks_7d', 
    'ACOS_yesterday', 
    'ACOS_7d', 
    'country_avg_ACOS_1m', 
    'total_clicks_30d', 
    'total_sales14d_30d', 
    'Reason'
]]

# 保存结果到 CSV 文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_v1_1_ES_2024-06-121.csv'
output_df.to_csv(output_path, index=False)

print(f"输出结果保存在 {output_path}")