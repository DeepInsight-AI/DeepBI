# filename: auto_keyword_optimization.py

import pandas as pd

# Load the dataset
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\预处理.csv"
df = pd.read_csv(file_path)

# Define a function to categorize and adjust bids based on the criteria
def adjust_bids(row):
    ACOS_7d = row['ACOS_7d']
    ACOS_30d = row['ACOS_30d']
    total_clicks_7d = row['total_clicks_7d']
    total_clicks_30d = row['total_clicks_30d']
    total_sales14d_7d = row['total_sales14d_7d']
    total_sales14d_30d = row['total_sales14d_30d']
    keywordBid = row['keywordBid']
    
    if 0.27 < ACOS_7d < 0.5:
        if 0 < ACOS_30d < 0.27:
            return keywordBid - 0.03, "降低竞价0.03"
        elif 0.27 < ACOS_30d < 0.5:
            return keywordBid - 0.04, "降低竞价0.04"
        elif ACOS_30d > 0.5:
            return keywordBid - 0.05, "降低竞价0.05"
    elif ACOS_7d > 0.5:
        if 0 < ACOS_30d < 0.27:
            return keywordBid - 0.05, "降低竞价0.05"
    if total_sales14d_7d == 0 and total_clicks_7d > 20 and 0.27 < ACOS_30d < 0.5:
        return keywordBid - 0.04, "降低竞价0.04"
    if total_sales14d_30d == 0 and total_clicks_7d > 0 and total_clicks_30d > 20:
        return "关闭", "关闭该词"
    if total_sales14d_7d == 0 and total_clicks_7d > 0 and ACOS_30d > 0.5:
        return "关闭", "关闭该词"
    if ACOS_7d > 0.5 and ACOS_30d > 0.27:
        return "关闭", "关闭该词"
    return keywordBid, ""

# Apply the adjust_bids function
df[['New Bid', 'Adjust Reason']] = df.apply(adjust_bids, axis=1, result_type="expand")

# Select relevant columns
result_df = df[['campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New Bid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'Adjust Reason']]

# Filter out rows without any adjustments
result_df = result_df[result_df['Adjust Reason'] != ""]

# Save the result to a new CSV file
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_1_LAPASA_ES_2024-07-03.csv"
result_df.to_csv(output_path, index=False)

print(f"Output saved to {output_path}")