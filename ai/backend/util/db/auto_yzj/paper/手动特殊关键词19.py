# filename: process_keywords_v1_1.py

import pandas as pd

# Step 1: Read the CSV file
csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
data = pd.read_csv(csv_path)

# Step 2: Filter the data
filtered_data = data[
    (data['total_sales_15d'] == 0) & 
    (data['total_clicks_7d'] <= 12)
]

# Step 3: Adjust keyword bids by adding 0.02
filtered_data['new_keywordBid'] = filtered_data['keywordBid'] + 0.02

# Step 4: Add reason column
filtered_data['reason'] = 'Ad group\'s total_sales_15d is 0 and total_clicks_7d is <= 12'

# Step 5: Save the filtered and adjusted data to a new CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_v1_1_IT_2024-06-19.csv'
filtered_data.to_csv(output_path, index=False, encoding='utf-8')

print(f"Filtered and adjusted data has been saved to {output_path}")