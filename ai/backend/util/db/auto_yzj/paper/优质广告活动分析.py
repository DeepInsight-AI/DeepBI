# filename: 优质广告活动分析.py

import pandas as pd

# 读取CSV文件
csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(csv_file_path)

# 过滤满足条件的优质广告活动
good_campaigns = df[
    (df['ACOS_7d'] < 0.27) &
    (df['ACOS_yesterday'] < 0.27) &
    (df['cost_yesterday'] > 0.8 * df['Budget'])
]

# 增加预算（原来预算的1/5，直到预算为50）
def increase_budget(row):
    new_budget = row['Budget'] * 1.2
    if new_budget > 50:
        new_budget = 50
    return new_budget

good_campaigns['New_Budget'] = good_campaigns.apply(increase_budget, axis=1)

# 增加原因列
good_campaigns['reason'] = 'Good performance: 7d ACOS < 0.27, Yesterday ACOS < 0.27, Cost Yesterday > 80% of Budget'

# 输出结果到新的CSV文件
output_csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_v1_1_LAPASA_IT_2024-07-03.csv'
good_campaigns.to_csv(output_csv_file_path, index=False, columns=[
    'campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday',
    'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 'country_avg_ACOS_1m',
    'total_clicks_30d', 'total_sales14d_30d', 'reason'
])

print(f"Filtered good campaigns data saved to '{output_csv_file_path}' successfully.")