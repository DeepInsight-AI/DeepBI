# filename: identify_poor_performing_campaigns.py

import pandas as pd

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# Define conditions for Definition One and Two
condition_def_one = (
    (df['ACOS7d'] > 0.24) &
    (df['ACOSYesterday'] > 0.24) &
    (df['costYesterday'] > 5.5) &
    (df['ACOS30d'] > df['countryAvgACOS1m'])
)

condition_def_two = (
    (df['ACOS30d'] > 0.24) &
    (df['ACOS30d'] > df['countryAvgACOS1m']) &
    (df['totalSales7d'] == 0) &
    (df['totalCost7d'] > 10)
)

# Filter the dataframe based on conditions
poor_performing_df = df[condition_def_one | condition_def_two].copy()

# Function to adjust the budget
def adjust_budget(row):
    if condition_def_one.loc[row.name]:
        if row['campaignBudget'] > 13:
            new_budget = max(8, row['campaignBudget'] - 5)
        else:
            new_budget = row['campaignBudget']
        reason = '定义一'
    elif condition_def_two.loc[row.name]:
        new_budget = max(5, row['campaignBudget'] - 5)
        reason = '定义二'
    else:
        new_budget = row['campaignBudget']
        reason = '未定义的情况'
    
    return pd.Series([new_budget, reason])

# Apply the budget adjustment function
new_budget_reason = poor_performing_df.apply(adjust_budget, axis=1)
poor_performing_df['New Budget'] = new_budget_reason.iloc[:, 0]
poor_performing_df['原因'] = new_budget_reason.iloc[:, 1]

# Filter the necessary columns to output
output_df = poor_performing_df[[
    'campaignId', 'campaignName', 'campaignBudget', 'New Budget', 'clicksYesterday', 
    'ACOS7d', 'totalClicks7d', 'totalSales7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d',
    'countryAvgACOS1m', '原因'
]]

# Save the results to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_UK_2024-07-14.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Results saved to {output_file_path}")