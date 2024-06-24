# filename: increase_budget.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义增加预算的函数
def increase_budget(row, increase_factor=0.2, max_budget=50):
    new_budget = row['Budget'] * (1 + increase_factor)
    if new_budget > max_budget:
        new_budget = max_budget
    return new_budget

# 筛选符合条件的广告活动
good_campaigns = data[
    (data['ACOS_7d'] < 0.24) &
    (data['ACOS_yesterday'] < 0.24) &
    (data['cost_yesterday'] > data['Budget'] * 0.8)
]

# 增加原来预算的1/5，直到预算为50
good_campaigns['New_Budget'] = good_campaigns.apply(increase_budget, axis=1)

# 添加原因字段
good_campaigns['Reason'] = 'Performance is good based on 7d ACOS and yesterday ACOS and yesterday cost exceeds 80% of budget'

# 选择需要的字段
output_columns = [
    'campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday',
    'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d',
    'total_clicks_30d', 'total_sales14d_30d', 'Reason'
]
output_data = good_campaigns[output_columns]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_v1_1_IT_2024-06-17.csv'
output_data.to_csv(output_file_path, index=False)

print(f"数据已成功保存到 {output_file_path}")