# filename: perform_budget_optimization.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选表现很好的优质广告活动
conditions = (
    (data['ACOS_yesterday'] < 0.27) &
    (data['ACOS_7d'] < 0.27) &
    (data['cost_yesterday'] > 0.8 * data['Budget'])
)
good_campaigns = data[conditions]

# 增加预算
good_campaigns['New Budget'] = good_campaigns['Budget'] * 1.2

# 限制预算不超过50
good_campaigns['New Budget'] = good_campaigns['New Budget'].apply(lambda x: min(x, 50))

# 添加增加预算的原因
good_campaigns['Reason'] = 'Recent performance meets all criteria'

# 输出所需的字段
output_columns = [
    'campaignId', 'campaignName', 'Budget', 'New Budget', 'cost_yesterday', 'clicks_yesterday', 
    'ACOS_yesterday', 'ACOS_7d', 'country_avg_ACOS_1m', 'total_clicks_30d', 'total_sales14d_30d', 'Reason'
]
output_data = good_campaigns[output_columns]

# 保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_v1_1_LAPASA_IT_2024-07-03.csv'
output_data.to_csv(output_file_path, index=False)

print(f'Data saved to {output_file_path}')