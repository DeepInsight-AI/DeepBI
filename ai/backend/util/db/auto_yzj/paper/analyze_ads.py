# filename: analyze_ads.py

import pandas as pd

# Load the data from the CSV file
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
df = pd.read_csv(file_path)

# Display the first few rows to understand the structure
print("Data Loaded: ")
print(df.head())

# Define conditions for identifying poor-performing campaigns
cond_1 = (
    (df['ACOS7d'] > 0.24) & 
    (df['ACOSYesterday'] > 0.24) & 
    (df['costYesterday'] > 5.5) & 
    (df['ACOS30d'] > df['countryAvgACOS1m'])
)

cond_2 = (
    (df['ACOS30d'] > 0.24) & 
    (df['ACOS30d'] > df['countryAvgACOS1m']) & 
    (df['totalSales7d'] == 0) & 
    (df['totalCost7d'] > 10)
)

# Filter campaigns based on conditions
poor_campaigns = df[cond_1 | cond_2]

# Apply budget adjustments
def adjust_budget(row):
    if cond_1.loc[row.name]:
        if row['campaignBudget'] > 13:
            new_budget = max(row['campaignBudget'] - 5, 8)
        else:
            new_budget = row['campaignBudget']
        reason = 'Definition 1'
    elif cond_2.loc[row.name]:
        if row['campaignBudget'] > 5:
            new_budget = 5
        else:
            new_budget = row['campaignBudget']
        reason = 'Definition 2'
    else:
        new_budget = row['campaignBudget']
        reason = 'No Adjustment'
    return pd.Series([new_budget, reason])

# Calculate new budget and reason for each poor-performing campaign
poor_campaigns[['New Budget', 'Reason']] = poor_campaigns.apply(adjust_budget, axis=1)

# Select the columns to output
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'New Budget', 
    'clicksYesterday', 'ACOSYesterday', 'totalClicks7d', 'totalSales7d',
    'ACOS7d', 'totalClicks30d', 'totalSales30d', 'ACOS30d', 'countryAvgACOS1m', 'Reason'
]

# Create a DataFrame with the selected columns
result_df = poor_campaigns[output_columns]

# Define the path to save the result
result_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_FR_2024-07-12.csv"

# Save the result to a CSV file
result_df.to_csv(result_file_path, index=False)

# Inform the user that the results have been saved successfully
print(f"Results saved successfully to {result_file_path}")