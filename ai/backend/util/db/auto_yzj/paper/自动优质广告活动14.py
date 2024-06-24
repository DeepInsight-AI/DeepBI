# filename: update_budget.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合条件的广告活动
filtered_data = data[
    (data['ACOS_7d'] < 0.24) &
    (data['ACOS_yesterday'] < 0.24) &
    (data['cost_yesterday'] > 0.8 * data['Budget'])
]

# 增加预算并记录原因
filtered_data['New_Budget'] = (filtered_data['Budget'] * 1.2).clip(upper=50)
filtered_data['Reason'] = '7d ACOS < 0.24, Yesterday ACOS < 0.24, Spend > 80% of Budget'

# 选择所需的列
result = filtered_data[[
    'campaignId',
    'campaignName',
    'Budget',
    'New_Budget',
    'cost_yesterday',
    'clicks_yesterday',
    'ACOS_yesterday',
    'ACOS_7d',
    'ACOS_30d',
    'total_clicks_30d',
    'total_sales14d_30d',
    'Reason'
]]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_v1_1_ES_2024-06-14.csv'
result.to_csv(output_file_path, index=False)

print("处理完成，结果已保存到:", output_file_path)