# filename: optimize_budget.py

import pandas as pd

# Load the CSV file
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/sd广告/预算优化/预处理.csv'
df = pd.read_csv(file_path)

# Filter campaigns based on the criteria
good_campaigns = df[
    (df['ACOS7d'] < 0.24) & 
    (df['ACOSYesterday'] < 0.24) & 
    (df['costYesterday'] > 0.8 * df['campaignBudget'])
]

# Adjust the budget
def adjust_budget(row):
    new_budget = row['campaignBudget'] * 1.2
    return min(new_budget, 50)

good_campaigns['New Budget'] = good_campaigns.apply(adjust_budget, axis=1)

# Extract relevant fields and reasons for adjustment
output_df = good_campaigns[[
    'campaignId',
    'campaignName',
    'campaignBudget',
    'New Budget',
    'costYesterday',
    'clicksYesterday',
    'ACOSYesterday',
    'ACOS7d',
    'ACOS30d',
    'totalClicks30d',
    'totalSales30d'
]]

output_df['Reason for Adjustment'] = 'High performance campaign based on defined criteria'

# Save results to a new CSV file
output_file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/sd广告/预算优化/提问策略/SD_优质sd广告活动_v1_1_LAPASA_FR_2024-07-14.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Results have been saved to {output_file_path}")