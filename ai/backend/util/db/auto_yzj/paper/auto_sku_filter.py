# filename: auto_sku_filter.py

import pandas as pd

# Step 1: Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: Define conditions for each definition
conditions = [
    (
        (data['ORDER_1m'] < 8) &
        (data['ACOS_7d'] > 0.24) &
        (data['total_clicks_7d'] > 13)
    ),
    (
        (data['ORDER_1m'] < 8) &
        (data['ACOS_30d'] > 0.24) &
        (data['total_sales14d_7d'] == 0) &
        (data['total_clicks_7d'] > 13)
    ),
    (
        (data['ORDER_1m'] < 8) &
        (data['ACOS_7d'].between(0.24, 0.5)) &
        (data['ACOS_30d'].between(0, 0.24)) &
        (data['total_clicks_7d'] > 13)
    ),
    (
        (data['ORDER_1m'] < 8) &
        (data['ACOS_7d'] > 0.24) &
        (data['ACOS_30d'] > 0.24)
    ),
    (data['ACOS_7d'] > 0.5),
    (
        (data['total_clicks_30d'] > 13) &
        (data['total_sales14d_30d'] == 0)
    ),
    (
        (data['total_clicks_7d'] >= 19) &
        (data['total_sales14d_7d'] == 0)
    )
]

# Step 3: Apply conditions to filter data and mark the definitions they satisfy
data['满足的定义'] = ''

for idx, condition in enumerate(conditions, start=1):
    data['满足的定义'] = data.apply(lambda row, cond=condition, def_num=idx: f"定义{def_num}" if cond.loc[row.name] else row['满足的定义'], axis=1)

# Filter out rows that satisfy at least one condition
filtered_data = data[data['满足的定义'] != '']

# Select specified columns for output
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 
    'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义'
]
output_data = filtered_data[output_columns]

# Step 4: Save the output to a CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_v1_1_ES_2024-06-20.csv'
output_data.to_csv(output_file_path, index=False)

print(f"Output saved to {output_file_path}")