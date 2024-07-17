# filename: budget_increase_analysis.py

import pandas as pd

# File paths
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_IT_2024-07-13.csv'

# Load the data
df = pd.read_csv(input_file)

# Filtering conditions
condition = (
    (df['ACOS7d'] < 0.24) &
    (df['ACOSYesterday'] < 0.24) &
    (df['costYesterday'] > 0.8 * df['campaignBudget'])
)

# Select rows that meet the conditions
qualified_campaigns = df[condition].copy()

# Increase budget by 0.2x the original budget until budget reaches 50
qualified_campaigns['NewBudget'] = qualified_campaigns['campaignBudget'].apply(lambda x: min(x * 1.2, 50))

# Reason for budget increase
qualified_campaigns['Reason'] = (
    f"ACOS7d < 0.24, ACOSYesterday < 0.24, costYesterday > 80% of campaignBudget"
)

# Selecting relevant columns for output
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'NewBudget',
    'costYesterday', 'clicksYesterday', 'ACOSYesterday', 
    'ACOS7d', 'totalClicks30d', 'totalSales30d', 'Reason'
]

output_df = qualified_campaigns.loc[:, output_columns]

# Save to CSV
output_df.to_csv(output_file, index=False)

print("Analysis complete. Results have been saved to the output CSV file.")