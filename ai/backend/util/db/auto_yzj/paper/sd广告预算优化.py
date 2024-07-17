# filename: sd广告预算优化.py

import pandas as pd

# Step 1: Data Preparation
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\预处理.csv"
data = pd.read_csv(file_path)

# Step 2: Identifying Well-Performing Campaigns
well_performing = data[(data['ACOS7d'] < 0.24) & (data['ACOSYesterday'] < 0.24) & (data['costYesterday'] > 0.8 * data['campaignBudget'])]

# Step 3: Adjusting Budget
def adjust_budget(campaignBudget):
    newBudget = campaignBudget
    while newBudget < 50:
        newBudget *= 1.2
        if newBudget > 50:
            newBudget = 50
    return newBudget

well_performing['newBudget'] = well_performing['campaignBudget'].apply(adjust_budget)

# Step 4: Output Result
output_columns = [
    'campaignId',
    'campaignName',
    'campaignBudget',
    'newBudget',
    'costYesterday',
    'clicksYesterday',
    'ACOSYesterday',
    'ACOS7d',
    'ACOS30d',
    'totalClicks30d',
    'totalSales30d'
]
reasons = "Performance meets criteria with ACOS7d < 0.24, ACOSYesterday < 0.24, CostYesterday > 80% of Budget."
well_performing['reason'] = reasons

output_filepath = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\提问策略\\SD_优质sd广告活动_v1_1_LAPASA_FR_2024-07-11.csv"
well_performing.to_csv(
    output_filepath,
    columns=output_columns + ['reason'],
    index=False
)
print("任务已完成，结果已保存。")