# filename: process_poor_performers.py

import pandas as pd
import numpy as np

# File path
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_ES_2024-07-10.csv'

# Read CSV file
data = pd.read_csv(file_path)

# Initialize columns for new bid and action reason
data['New_keywordBid'] = np.nan
data['Action_Reason'] = np.nan

# Apply the conditions
# Condition 1
cond1 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] <= 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.5)
data.loc[cond1, 'New_keywordBid'] = data['keywordBid'] / ((data['ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[cond1, 'Action_Reason'] = 'Bid Adjust: ACOS_7d > 0.24 and <= 0.5 and ACOS_30d > 0 and <= 0.5'

# Condition 2
cond2 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36)
data.loc[cond2, 'New_keywordBid'] = data['keywordBid'] / ((data['ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[cond2, 'Action_Reason'] = 'Bid Adjust: ACOS_7d > 0.5 and ACOS_30d <= 0.36'

# Condition 3
cond3 = (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36)
data.loc[cond3, 'New_keywordBid'] = data['keywordBid'] - 0.04
data.loc[cond3, 'Action_Reason'] = 'Lower Bid: Clicks_7d >= 10 and Sales_7d = 0 and ACOS_30d <= 0.36'

# Condition 4
cond4 = (data['total_clicks_7d'] > 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5)
data.loc[cond4, 'New_keywordBid'] = '关闭'
data.loc[cond4, 'Action_Reason'] = 'Pause: Clicks_7d > 10 and Sales_7d = 0 and ACOS_30d > 0.5'

# Condition 5
cond5 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36)
data.loc[cond5, 'New_keywordBid'] = '关闭'
data.loc[cond5, 'Action_Reason'] = 'Pause: ACOS_7d > 0.5 and ACOS_30d > 0.36'

# Condition 6
cond6 = (data['total_sales14d_30d'] == 0) & (data['total_cost_30d'] >= 5)
data.loc[cond6, 'New_keywordBid'] = '关闭'
data.loc[cond6, 'Action_Reason'] = 'Pause: Sales_30d = 0 and Cost_30d >= 5'

# Condition 7
cond7 = (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15) & (data['total_clicks_7d'] > 0)
data.loc[cond7, 'New_keywordBid'] = '关闭'
data.loc[cond7, 'Action_Reason'] = 'Pause: Sales_30d = 0 and Clicks_30d >= 15 and Clicks_7d > 0'

# Select relevant columns and filter rows with actions
result = data.dropna(subset=['New_keywordBid']).copy()

result = result[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
                 'keywordBid', 'New_keywordBid', 'targeting', 
                 'total_cost_yesterday', 'total_clicks_yesterday', 'total_sales14d_yesterday', 
                 'total_cost_7d', 'total_sales14d_7d', 'total_cost_30d', 
                 'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', 'Action_Reason']]

# Save the result to a CSV file
result.to_csv(output_file_path, index=False)

# Output the resulting DataFrame to verify
print(result.head())