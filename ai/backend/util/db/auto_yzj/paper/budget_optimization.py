# filename: budget_optimization.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义一和定义二的判断条件
def condition_one(row):
    return (row['ACOS7d'] > 0.24) and (row['ACOSYesterday'] > 0.24) and \
           (row['costYesterday'] > 5.5) and (row['ACOS30d'] > row['countryAvgACOS1m'])

def condition_two(row):
    return (row['ACOS30d'] > 0.24) and (row['ACOS30d'] > row['countryAvgACOS1m']) and \
           (row['totalSales7d'] == 0) and (row['totalCost7d'] > 10)

# 处理数据
def adjust_budget(row):
    if condition_one(row):
        if row['campaignBudget'] > 13:
            new_budget = max(row['campaignBudget'] - 5, 8)
            reason = "Condition One 预算降低"
        elif row['campaignBudget'] < 8:
            new_budget = row['campaignBudget']
            reason = "Condition One 预算不变"
        else:
            new_budget = row['campaignBudget']
            reason = "Condition One"
    elif condition_two(row):
        if row['campaignBudget'] > 5:
            new_budget = max(row['campaignBudget'] - 5, 5)
            reason = "Condition Two 预算降低"
        else:
            new_budget = "关闭"
            reason = "Condition Two 关闭"
    else:
        new_budget = row['campaignBudget']
        reason = ""

    return pd.Series([new_budget, reason])

adjustments = data.apply(adjust_budget, axis=1)
data['New Budget'], data['Reason'] = adjustments[0], adjustments[1]

# 筛选出需要进行预算调整的广告活动
result = data[data['Reason'] != ""]

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_IT_2024-07-12.csv'
result.to_csv(output_path, index=False, columns=[
    'campaignId', 'campaignName', 'campaignBudget', 'New Budget', 'clicksYesterday',
    'ACOSYesterday', 'ACOS7d', 'totalClicks7d', 'totalSales7d', 'ACOS30d',
    'totalClicks30d', 'totalSales30d', 'countryAvgACOS1m', 'Reason'
])

print(f"Results have been saved to {output_path}")