# filename: search_terms_analysis.py

import pandas as pd

# Load the CSV file
csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
df = pd.read_csv(csv_file_path)

# Apply Definition 1
definition_1 = (df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)

# Apply Definition 2
# Note: There is no explicit orders column in the provided csv file field descriptions.
# Assuming "orders" field for this operation.
definition_2 = (df['total_clicks_30d'] >= 2) & (df['ACOS_30d'] < 0.24)

# Combine both definitions using OR condition
condition = definition_1 | definition_2

# Extract the relevant columns and reasons
result = df[condition].copy()
result.loc[definition_1, 'reason'] = '定义一'
result.loc[definition_2, 'reason'] = '定义二'

# Select only the required columns
output_columns = [
    'campaignName', 'campaignId', 'adGroupName', 'adGroupId', 
    'ACOS_7d', 'total_sales14d_7d', 'total_clicks_30d', 'ACOS_30d', 
    'searchTerm', 'reason'
]
result = result[output_columns]

# Save the results to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_v1_1_IT_2024-06-17.csv'
result.to_csv(output_file_path, index=False)

print("Results saved to:", output_file_path)