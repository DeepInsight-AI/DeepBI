# filename: update_campaign_budget.py

import pandas as pd

# Load the CSV data
csv_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
data = pd.read_csv(csv_path)

# Filtering campaigns based on conditions
filtered_data = data[(data['ACOS7d'] < 0.24) & 
                     (data['ACOSYesterday'] < 0.24) & 
                     (data['costYesterday'] > 0.8 * data['campaignBudget'])]

# Function to calculate new budget
def adjust_budget(current_budget):
    new_budget = current_budget
    while new_budget < 50:
        new_budget += 0.2 * current_budget
        if new_budget >= 50:
            new_budget = 50
            break
    return new_budget

# Apply budget adjustment
filtered_data['New Budget'] = filtered_data['campaignBudget'].apply(adjust_budget)

# Select relevant columns and add reasons
output_data = filtered_data[['campaignId', 'campaignName', 'campaignBudget', 'New Budget', 'costYesterday', 'clicksYesterday', 'ACOSYesterday', 'ACOS7d', 'ACOS30d', 'totalSales30d', 'totalClicks30d']]
output_data['Reason'] = 'Met criteria: ACOS7d < 0.24, ACOSYesterday < 0.24, CostYesterday > 80% of Budget'

# Output to CSV
output_csv_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_US_2024-07-15.csv"
output_data.to_csv(output_csv_path, index=False)

print("Output generated successfully at:", output_csv_path)