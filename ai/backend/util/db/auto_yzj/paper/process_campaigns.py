# filename: process_campaigns.py

import pandas as pd

# Load Data
csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(csv_path)

# Filter data based on Definition 1
definition_1 = data[
    (data['ACOS7d'] > 0.24) &
    (data['ACOSYesterday'] > 0.24) &
    (data['costYesterday'] > 5.5) &
    (data['ACOS30d'] > data['countryAvgACOS1m'])
]

# Filter data based on Definition 2
definition_2 = data[
    (data['ACOS30d'] > 0.24) &
    (data['ACOS30d'] > data['countryAvgACOS1m']) &
    (data['totalSales7d'] == 0) &
    (data['totalCost7d'] > 10)
]

# Columns to output
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'costYesterday', 
    'clicksYesterday', 'salesYesterday', 'totalCost7d', 
    'totalSales7d', 'totalClicks7d', 'totalCost30d', 
    'totalSales30d', 'totalClicks30d', 'ACOS30d', 
    'ACOS7d', 'ACOSYesterday', 'countryAvgACOS1m'
]

# Function to Adjust Budget and add Reason
def adjust_budget(row, definition):
    if definition == 1:
        reason = "Definition 1"
        if row['campaignBudget'] > 13:
            new_budget = max(row['campaignBudget'] - 5, 8)
        else:
            new_budget = row['campaignBudget']
    elif definition == 2:
        reason = "Definition 2"
        new_budget = max(row['campaignBudget'] - 5, 5)
    return new_budget, reason

# Apply budget rules and collect the results
results = []

for _, row in definition_1.iterrows():
    new_budget, reason = adjust_budget(row, 1)
    results.append({
        **row[output_columns].to_dict(),
        'New Budget': new_budget,
        'Reason': reason
    })

for _, row in definition_2.iterrows():
    new_budget, reason = adjust_budget(row, 2)
    results.append({
        **row[output_columns].to_dict(),
        'New Budget': new_budget,
        'Reason': reason
    })

# Create a DataFrame of results
results_df = pd.DataFrame(results)

# Save results to CSV
output_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_US_2024-07-16.csv'
results_df.to_csv(output_csv_path, index=False)

print(f"Results saved to {output_csv_path}")