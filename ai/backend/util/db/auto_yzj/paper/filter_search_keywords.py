# filename: filter_search_keywords.py

import pandas as pd

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_优质商品投放搜索词_v1_1_ES_2024-06-26.csv'

df = pd.read_csv(file_path)

# Filter according to Definition 1 and Definition 2
definition_1 = (df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)
definition_2 = (df['ORDER_1m'] > 0) & (df['ACOS_30d'] < 0.24)

# Identify the corresponding reasons
df_def1 = df[definition_1].copy()
df_def1['reason'] = 'Definition 1'

df_def2 = df[definition_2].copy()
df_def2['reason'] = 'Definition 2'

# Combine filtered data
result_df = pd.concat([df_def1, df_def2])

# Select and rename the needed columns
result_df = result_df[['campaignName', 'adGroupName', 'ACOS_7d', 'total_sales14d_7d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']]
result_df.rename(columns={
    'campaignName': 'Campaign Name',
    'adGroupName': 'adGroupName',
    'ACOS_7d': 'week_acos',
    'total_sales14d_7d': 'total_sales_7d',
    'ORDER_1m': 'total_orders_1m',
    'ACOS_30d': 'acos_30d',
    'searchTerm': 'searchTerm',
    'reason': 'reason'
}, inplace=True)

# Save the result to CSV
result_df.to_csv(output_file_path, index=False)
print(f"Filtered data has been saved to {output_file_path}")