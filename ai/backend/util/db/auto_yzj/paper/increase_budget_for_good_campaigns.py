# filename: increase_budget_for_good_campaigns.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义预算增加因子和上限
increase_factor = 0.2
max_budget = 50

# 选择表现很好的优质广告活动
good_campaigns = data[
    (data['ACOS7d'] < 0.24) &
    (data['ACOSYesterday'] < 0.24) &
    (data['costYesterday'] > 0.8 * data['campaignBudget'])
].copy()

# 调整预算
good_campaigns['New_Budget'] = good_campaigns['campaignBudget'] * (1 + increase_factor)
good_campaigns.loc[good_campaigns['New_Budget'] > max_budget, 'New_Budget'] = max_budget

# 增加新字段
good_campaigns['Reason'] = 'Increased budget due to good performance and high costs'

# 过滤输出字段
output_columns = [
    'campaignId',
    'campaignName',
    'campaignBudget',
    'New_Budget',
    'costYesterday',
    'clicksYesterday',
    'ACOSYesterday',
    'ACOS7d',
    'ACOS30d',
    'totalClicks30d',
    'totalSales30d',
    'Reason'
]
result = good_campaigns[output_columns]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_IT_2024-07-15.csv'
result.to_csv(output_file_path, index=False)

print("结果已成功保存至文件: ", output_file_path)