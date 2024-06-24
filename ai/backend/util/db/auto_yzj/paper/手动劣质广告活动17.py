# filename: ad_campaign_analysis.py
import pandas as pd

# Constants
INPUT_CSV = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
OUTPUT_CSV = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_v1_1_IT_2024-06-17.csv'

# Load dataset
df = pd.read_csv(INPUT_CSV)

# Initialize New_Budget column with current Budget
df['New_Budget'] = df['Budget']

def adjust_budget(row):
    reason = ""
    if (row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and 
        row['clicks_yesterday'] >= 10 and row['ACOS_30d'] > row['country_avg_ACOS_1m']):
        while row['New_Budget'] > 8:
            row['New_Budget'] -= 5
        reason = "定义一"

    elif (row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and 
          row['cost_yesterday'] > 0.8 * row['Budget'] and row['ACOS_30d'] > row['country_avg_ACOS_1m']):
        while row['New_Budget'] > 8:
            row['New_Budget'] -= 5
        reason = "定义二"

    elif (row['ACOS_30d'] > 0.24 and row['ACOS_30d'] > row['country_avg_ACOS_1m'] and 
          row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] >= 15):
        while row['New_Budget'] > 5:
            row['New_Budget'] -= 5
        reason = "定义三"

    elif (row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 75):
        reason = "定义四"
        row['New_Budget'] = "关闭"
    return reason

# Apply the budget adjustment function and generate reasons
df['Reason'] = df.apply(adjust_budget, axis=1)

# Filter out only the campaigns that were tagged with any reason
result_df = df[df['Reason'] != ""]

# Select and rename columns for the output file
columns_to_output = ['campaignId', 'campaignName', 'Budget', 'New_Budget', 'clicks_yesterday', 
                     'ACOS_yesterday', 'ACOS_7d', 'total_clicks_7d', 'total_sales14d_7d', 
                     'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d', 'country_avg_ACOS_1m', 'Reason']
result_df = result_df[columns_to_output]

# Save the resulting dataframe to the output CSV file
result_df.to_csv(OUTPUT_CSV, index=False)

print(f"Results have been saved to {OUTPUT_CSV}")