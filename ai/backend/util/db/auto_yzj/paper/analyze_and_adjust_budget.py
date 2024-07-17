# filename: analyze_and_adjust_budget.py

import pandas as pd

# 1. 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 2. 定义条件
def condition1(row):
    return (row['ACOS7d'] > 0.24) and (row['ACOSYesterday'] > 0.24) and (row['costYesterday'] > 5.5) and (row['ACOS30d'] > row['countryAvgACOS1m'])

def condition2(row):
    return (row['ACOS30d'] > 0.24) and (row['ACOS30d'] > row['countryAvgACOS1m']) and (row['totalSales7d'] == 0) and (row['totalCost7d'] > 10)

# 3. 识别并调整预算
results = []

def adjust_budget(row):
    original_budget = row['campaignBudget']
    new_budget = original_budget
    reason = ""
    
    if condition1(row):
        if original_budget > 13:
            new_budget = max(8, original_budget - 5)
        reason = "Condition 1 met"
    elif condition2(row):
        if original_budget > 10:
            new_budget = max(5, original_budget - 5)
        reason = "Condition 2 met"
    
    if reason:
        results.append({
            'campaignId': row['campaignId'],
            'campaignName': row['campaignName'],
            'Budget': original_budget,
            'New Budget': new_budget,
            'clicksYesterday': row['clicksYesterday'],
            'ACOSYesterday': row['ACOSYesterday'],
            'ACOS7d': row['ACOS7d'],
            'totalClicks7d': row['totalClicks7d'],
            'totalSales7d': row['totalSales7d'],
            'ACOS30d': row['ACOS30d'],
            'totalClicks30d': row['totalClicks30d'],
            'totalSales30d': row['totalSales30d'],
            'countryAvgACOS1m': row['countryAvgACOS1m'],
            'Reason': reason
        })

data.apply(adjust_budget, axis=1)

# 4. 输出结果
result_df = pd.DataFrame(results)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_IT_2024-07-11.csv'
result_df.to_csv(output_path, index=False)

print("分析完成，结果已保存到:", output_path)