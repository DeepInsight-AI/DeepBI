# filename: get_poor_performance_campaigns.py
import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# Convert field names to English for easier processing
data.columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'market', 'costYesterday', 'clicksYesterday',
    'salesYesterday', 'totalCost7d', 'totalSales7d', 'totalCost30d', 'totalSales30d',
    'totalClicks30d', 'totalClicks7d', 'ACOS30d', 'ACOS7d', 'ACOSYesterday', 'countryAvgACOS1m'
]

# Define conditions based on given definitions
condition_def1 = (
    (data['ACOS7d'] > 0.24) &
    (data['ACOSYesterday'] > 0.24) &
    (data['costYesterday'] > 5.5) &
    (data['ACOS30d'] > data['countryAvgACOS1m'])
)

condition_def2 = (
    (data['ACOS30d'] > 0.24) &
    (data['ACOS30d'] > data['countryAvgACOS1m']) &
    (data['totalSales7d'] == 0) &
    (data['totalCost7d'] > 10)
)

# Apply conditions
filtered_data_def1 = data[condition_def1].copy()
filtered_data_def2 = data[condition_def2].copy()

# Budget adjustment rules for definition one
def adjust_budget_def1(row):
    if row['campaignBudget'] > 13:
        new_budget = max(row['campaignBudget'] - 5, 8)
    else:
        new_budget = row['campaignBudget']
    return new_budget

# Budget adjustment rules for definition two
def adjust_budget_def2(row):
    if row['campaignBudget'] > 10:
        new_budget = max(row['campaignBudget'] - 5, 5)
    else:
        new_budget = row['campaignBudget']
    return new_budget

# Adjust budgets
filtered_data_def1['newBudget'] = filtered_data_def1.apply(adjust_budget_def1, axis=1)
filtered_data_def2['newBudget'] = filtered_data_def2.apply(adjust_budget_def2, axis=1)

# Add 'Reason for Change' column
filtered_data_def1['reasonForChange'] = 'Definition 1 - High ACOS and Cost'
filtered_data_def2['reasonForChange'] = 'Definition 2 - High ACOS, No Sales, and High Cost'

# Concatenate the filtered data for both definitions
result_data = pd.concat([filtered_data_def1, filtered_data_def2])

# Select relevant columns
result_data = result_data[[
    'campaignId', 'campaignName', 'campaignBudget', 'newBudget', 'clicksYesterday', 'ACOSYesterday', 
    'ACOS7d', 'totalClicks7d', 'totalSales7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d', 
    'countryAvgACOS1m', 'reasonForChange'
]]

# Save the result to a CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_UK_2024-07-11.csv'
result_data.to_csv(output_file_path, index=False)

print("Process completed. Results saved to:", output_file_path)