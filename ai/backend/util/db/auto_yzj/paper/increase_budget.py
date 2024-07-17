# filename: increase_budget.py

import pandas as pd

# 加载数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
data = pd.read_csv(file_path)

# 条件过滤
filtered_data = data[
    (data['ACOS7d'] < 0.24) & 
    (data['ACOSYesterday'] < 0.24) & 
    (data['costYesterday'] > 0.8 * data['campaignBudget'])
]

# 增加预算
def adjust_budget(row):
    original_budget = row['campaignBudget']
    new_budget = min(original_budget * 1.2, 50)
    return new_budget

filtered_data['NewBudget'] = filtered_data.apply(adjust_budget, axis=1)

# 选取需要的列
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'NewBudget', 'costYesterday',
    'clicksYesterday', 'ACOSYesterday', 'ACOS7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d'
]
result = filtered_data[output_columns]

# 增加预算原因
result['Reason'] = '符合优质广告活动的标准，增加预算'

# 保存结果
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_US_2024-07-12.csv"
result.to_csv(output_file_path, index=False)

print("任务完成，结果已保存到文件中。")