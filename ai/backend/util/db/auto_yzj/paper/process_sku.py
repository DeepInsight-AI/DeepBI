# filename: process_sku.py
import pandas as pd

# Step 1: Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path)

# Step 2: Define the conditions
conditions = [
    # Definition 1
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_7d'] > 0.24) &
        (df['total_cost_7d'] > 5)
    ),
    # Definition 2
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_30d'] > 0.24) &
        (df['total_sales_7d'] == 0) &
        (df['total_cost_7d'] > 5)
    ),
    # Definition 3
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) &
        (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) &
        (df['total_cost_7d'] > 5)
    ),
    # Definition 4
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_7d'] > 0.24) &
        (df['ACOS_30d'] > 0.24)
    ),
    # Definition 5
    (
        df['ACOS_7d'] > 0.5
    ),
    # Definition 6
    (
        (df['total_cost_30d'] > 5) &
        (df['total_sales_30d'] == 0)
    ),
    # Definition 7
    (
        (df['ORDER_1m'] < 8) &
        (df['total_cost_7d'] >= 5) &
        (df['total_sales_7d'] == 0)
    ),
    # Definition 8
    (
        (df['ORDER_1m'] >= 8) &
        (df['total_cost_7d'] >= 10) &
        (df['total_sales_7d'] == 0)
    )
]

# Step 3: Filter the DataFrame
filtered_df_list = []
for idx, condition in enumerate(conditions):
    filtered_df = df[condition].copy()
    filtered_df['matched_definition'] = idx + 1
    filtered_df_list.append(filtered_df)

result_df = pd.concat(filtered_df_list, ignore_index=True)

# Select required columns and save to new CSV
output_columns = [
    'campaignName', 'adId', 'adGroupName', 
    'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 
    'advertisedSku', 'ORDER_1m', 'matched_definition'
]

output_df = result_df[output_columns]
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_ES_2024-07-16.csv'
output_df.to_csv(output_path, index=False)

print(f"Filtered data saved to {output_path}")