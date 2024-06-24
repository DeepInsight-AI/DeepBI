# filename: sku_analysis.py

import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
data = pd.read_csv(file_path)

# Define conditions based on given definitions
def1 = (data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24)
def2 = (data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10)
def3 = (data['ACOS_7d'].between(0.24, 0.5)) & (data['ACOS_30d'].between(0, 0.24)) & (data['total_clicks_7d'] > 13)
def4 = (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24)
def5 = (data['ACOS_7d'] > 0.5)
def6 = (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)

# Combine all conditions
combined_cond = def1 | def2 | def3 | def4 | def5 | def6

# Apply the conditions to filter data
filtered_data = data[combined_cond].copy()

# Determine the reason for closure for each filtered SKU
filtered_data['closure_reason'] = ''
filtered_data.loc[def1, 'closure_reason'] = '近7天总点击数大于10，近7天的平均acos值在0.24以上'
filtered_data.loc[def2, 'closure_reason'] = '近30天的平均acos值大于0.24，近七天没有销售额，且近7天总点击数大于10'
filtered_data.loc[def3, 'closure_reason'] = '近7天的平均acos值在大于0.24小于0.5，近30天的平均acos值大于0小于0.24，且近7天的点击数大于13'
filtered_data.loc[def4, 'closure_reason'] = '近7天的平均acos值大于0.24，且近30天的平均acos值大于0.24'
filtered_data.loc[def5, 'closure_reason'] = '近7天的平均acos值大于0.5'
filtered_data.loc[def6, 'closure_reason'] = '近30天的总点击数大于13，并且没有销售额'

# Select relevant columns for the final output
output_columns = [
    'campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d',
    'total_clicks_7d', 'advertisedSku', 'closure_reason'
]
final_output = filtered_data[output_columns]

# Save to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\关闭SKU_FR.csv'
final_output.to_csv(output_path, index=False)

print("CSV file has been created successfully.")