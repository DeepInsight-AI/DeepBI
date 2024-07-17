# filename: SD_sku_filtering.py

import pandas as pd

# Load the CSV file
csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(csv_file_path)

# Define the conditions for each definition
conditions = [
    (
        (df['ORDER_1m'] < 8) & 
        (df['ACOS_7d'] > 0.24) & 
        (df['total_cost_7d'] > 5),
        "定义一"
    ),
    (
        (df['ORDER_1m'] < 8) & 
        (df['ACOS_30d'] > 0.24) & 
        (df['total_sales_7d'] == 0) & 
        (df['total_cost_7d'] > 5),
        "定义二"
    ),
    (
        (df['ORDER_1m'] < 8) & 
        (df['ACOS_7d'] > 0.24) & 
        (df['ACOS_7d'] < 0.5) & 
        (df['ACOS_30d'] > 0) & 
        (df['ACOS_30d'] < 0.24) & 
        (df['total_cost_7d'] > 5),
        "定义三"
    ),
    (
        (df['ORDER_1m'] < 8) & 
        (df['ACOS_7d'] > 0.24) & 
        (df['ACOS_30d'] > 0.24),
        "定义四"
    ),
    (
        (df['ACOS_7d'] > 0.5),
        "定义五"
    ),
    (
        (df['total_cost_30d'] > 5) & 
        (df['total_sales_30d'] == 0),
        "定义六"
    ),
    (
        (df['ORDER_1m'] < 8) & 
        (df['total_cost_7d'] >= 5) & 
        (df['total_sales_7d'] == 0),
        "定义七"
    ),
    (
        (df['ORDER_1m'] >= 8) & 
        (df['total_cost_7d'] >= 10) & 
        (df['total_sales_7d'] == 0),
        "定义八"
    ),
]

# Initialize a list to store the rows that match any definition
matched_rows = []

# Check each condition and add rows that match any of them to the list
for condition, definition in conditions:
    matches = df[condition]
    matches['满足的定义'] = definition  # Add a column to specify the matching definition
    matched_rows.append(matches)

# Concatenate all matched rows
result_df = pd.concat(matched_rows).drop_duplicates()

# Select the required columns for output
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 
    'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 
    'ORDER_1m', '满足的定义'
]
result_df = result_df[output_columns]

# Save the resulting DataFrame to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_IT_2024-07-11.csv'
result_df.to_csv(output_file_path, index=False)

print(f"Filtered data saved successfully to {output_file_path}")