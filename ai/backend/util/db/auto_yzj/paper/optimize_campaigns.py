# filename: optimize_campaigns.py

import pandas as pd

# Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# Filter for high-performing campaigns
filtered_df = df[
    (df['ACOS7d'] < 0.24) &
    (df['ACOSYesterday'] < 0.24) &
    (df['costYesterday'] > 0.8 * df['campaignBudget'])
]

# Increase the budget
def increase_budget(row):
    new_budget = row['campaignBudget'] * 1.2
    if new_budget > 50:
        return 50
    return new_budget

filtered_df['NewBudget'] = filtered_df.apply(increase_budget, axis=1)

# Select relevant columns
output_df = filtered_df[[
    'campaignId',
    'campaignName',
    'campaignBudget',
    'NewBudget',
    'costYesterday',
    'clicksYesterday',
    'ACOSYesterday',
    'ACOS7d',
    'ACOS30d',
    'totalClicks30d',
    'totalSales30d'
]].copy()

# Add a reason column for why budget was increased
output_df['Reason'] = 'High performance: ACOS7d < 0.24, ACOSYesterday < 0.24, and costYesterday > 80% of budget'

# Specify the output file path
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_IT_2024-07-16.csv'

# Save the result to CSV
output_df.to_csv(output_path, index=False)

print(f"High-performing campaigns have been saved to {output_path}.")