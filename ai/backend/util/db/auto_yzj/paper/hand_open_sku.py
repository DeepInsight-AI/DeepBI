# filename: hand_open_sku.py

import pandas as pd

# 1. Load the data from the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 2. Define the conditions
condition1_def1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24)
condition2_def1 = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)

condition1_def2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24)
condition2_def2 = data['total_clicks_7d'] == 0

# Applying the conditions for definitions
def1 = condition1_def1 & condition2_def1
def2 = condition1_def2 & condition2_def2

# 3. Select the rows that meet either Definition 1 or Definition 2
filtered_data = data[def1 | def2]

# 4. Add a column to specify which definition each row met
filtered_data['满足的定义'] = '定义一'
filtered_data.loc[def2, '满足的定义'] = '定义二'

# 5. Specify the columns to keep in the output
output_columns = [
    'campaignName', 
    'adId', 
    'adGroupName', 
    'ACOS_30d', 
    'ACOS_7d', 
    'total_clicks_7d', 
    'advertisedSku', 
    'ORDER_1m', 
    '满足的定义'
]

output_data = filtered_data[output_columns]

# 6. Save the output to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_ES_2024-06-27.csv'
output_data.to_csv(output_file_path, index=False)

print("CSV file has been generated successfully.")