# filename: search_term_optimization.py

import pandas as pd

# Load the dataset
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv"
data = pd.read_csv(file_path)

# Apply the conditions for Definition 1 and Definition 2
condition_1 = (data['total_clicks_30d'] > 13) & (data['total_cost_30d'] > 7) & (data['ORDER_1m'] == 0)
condition_2 = (data['total_clicks_7d'] > 10) & (data['total_cost_7d'] > 5) & (data['ORDER_7d'] == 0)

# Filter the DataFrame based on the conditions
filtered_data_1 = data[condition_1].copy()
filtered_data_1['reason'] = 'Definition 1'

filtered_data_2 = data[condition_2].copy()
filtered_data_2['reason'] = 'Definition 2'

# Combine the filtered results
filtered_data = pd.concat([filtered_data_1, filtered_data_2])

# Select relevant columns
output_data = filtered_data[[
    'campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_cost_7d',
    'total_clicks_30d', 'total_cost_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]]

# Save the result to a CSV file
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_FR_2024-07-14.csv"
output_data.to_csv(output_file_path, index=False)

print("Filtered data has been saved to:", output_file_path)