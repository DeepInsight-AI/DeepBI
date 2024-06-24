# filename: process_keyword_acos.py

import pandas as pd
import os

# Load the dataset
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\搜索词优化\\预处理.csv"
df = pd.read_csv(file_path)

# Filter the dataset based on the defined criteria
filtered_df = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

# Organize the resulting data
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm']]
result_df = result_df.rename(columns={
    'campaignName': 'Campaign Name',
    'adGroupName': 'adGroupName',
    'ACOS_7d': 'week_acos',
    'searchTerm': 'searchTerm'
})
result_df['reason'] = "近七天有销售额且ACOS值在0.2以下"

# Define the path for the result file
save_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\搜索词优化\\提问策略\\自动_优质搜索词_IT_2024-06-06.csv"

# Ensure the directory exists
os.makedirs(os.path.dirname(save_path), exist_ok=True)

# Save the result to a new CSV file
result_df.to_csv(save_path, index=False, encoding='utf_8_sig')

print("Processed data has been saved to", save_path)