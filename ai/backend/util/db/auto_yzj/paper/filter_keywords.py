# filename: filter_keywords.py

import pandas as pd

# Load the CSV file
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放搜索词优化/预处理.csv"
data = pd.read_csv(file_path)

# Filtering conditions
condition_1 = (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.36) & (data['ORDER_1m'] <= 5)
condition_2 = (data['ACOS_30d'] >= 0.36) & (data['ORDER_1m'] <= 8)
condition_3 = (data['total_clicks_30d'] > 13) & (data['ORDER_1m'] == 0)
condition_4 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.36) & (data['ORDER_7d'] <= 3)
condition_5 = (data['ACOS_7d'] >= 0.36) & (data['ORDER_7d'] <= 5)
condition_6 = (data['total_clicks_7d'] > 10) & (data['ORDER_7d'] == 0)

# Combine all conditions
combined_conditions = (
    condition_1 | condition_2 | condition_3 | 
    condition_4 | condition_5 | condition_6
)

# Apply combined conditions to filter the data
filtered_data = data[combined_conditions]

# Detect reason columns
filtered_data['reason'] = ''
filtered_data.loc[condition_1, 'reason'] = 'Definition 1'
filtered_data.loc[condition_2, 'reason'] = 'Definition 2'
filtered_data.loc[condition_3, 'reason'] = 'Definition 3'
filtered_data.loc[condition_4, 'reason'] = 'Definition 4'
filtered_data.loc[condition_5, 'reason'] = 'Definition 5'
filtered_data.loc[condition_6, 'reason'] = 'Definition 6'

# Create new DataFrame with required columns
output_data = filtered_data[[
    'campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 
    'ORDER_7d', 'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]]

# Save to CSV
output_file = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放搜索词优化/提问策略/手动_劣质_ASIN_搜索词_v1_1_LAPASA_ITES_2024-07-02.csv"
output_data.to_csv(output_file, index=False)

print("Filtered data has been successfully saved to:", output_file)