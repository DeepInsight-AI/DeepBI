# filename: filter_search_terms.py

import pandas as pd

# Load the CSV file
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\搜索词优化\\预处理.csv"
data = pd.read_csv(file_path)

# Define the conditions
condition1 = (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.36) & (data['total_sales14d_30d'] <= 5)
condition2 = (data['ACOS_30d'] >= 0.36) & (data['total_sales14d_30d'] <= 8)
condition3 = (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)
condition4 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.36) & (data['total_sales14d_7d'] <= 3)
condition5 = (data['ACOS_7d'] >= 0.36) & (data['total_sales14d_7d'] <= 5)
condition6 = (data['total_clicks_7d'] > 10) & (data['total_sales14d_7d'] == 0)

# Combine conditions
combined_condition = condition1 | condition2 | condition3 | condition4 | condition5 | condition6

# Filter data
filtered_data = data[combined_condition]

# Map reasons
reasons = {
    "condition1": "定义一",
    "condition2": "定义二",
    "condition3": "定义三",
    "condition4": "定义四",
    "condition5": "定义五",
    "condition6": "定义六",
}

filtered_data['reason'] = ""
filtered_data.loc[condition1, 'reason'] = reasons['condition1']
filtered_data.loc[condition2, 'reason'] = reasons['condition2']
filtered_data.loc[condition3, 'reason'] = reasons['condition3']
filtered_data.loc[condition4, 'reason'] = reasons['condition4']
filtered_data.loc[condition5, 'reason'] = reasons['condition5']
filtered_data.loc[condition6, 'reason'] = reasons['condition6']

# Columns to keep
columns_to_keep = [
    'campaignName',
    'campaignId',
    'adGroupName',
    'adGroupId',
    'total_clicks_7d',
    'ACOS_7d',
    'total_sales14d_7d',
    'total_clicks_30d',
    'total_sales14d_30d',
    'ACOS_30d',
    'searchTerm',
    'reason'
]

# Select relevant columns
filtered_data = filtered_data[columns_to_keep]

# Save to CSV
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\搜索词优化\\提问策略\\手动_劣质搜索词_v1_1_ES_2024-06-14.csv"
filtered_data.to_csv(output_path, index=False)

print("Filtered data has been saved to:", output_path)