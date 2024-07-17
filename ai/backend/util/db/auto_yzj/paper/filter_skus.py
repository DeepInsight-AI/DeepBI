# filename: filter_skus.py

import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path)

# Define the conditions for each definition
definition1 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_cost_7d'] > 5)
definition2 = (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales_7d'] == 0) & (df['total_cost_7d'] > 5)
definition3 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_cost_7d'] > 5)
definition4 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
definition5 = df['ACOS_7d'] > 0.5
definition6 = (df['total_cost_30d'] > 5) & (df['total_sales_30d'] == 0)
definition7 = (df['ORDER_1m'] < 8) & (df['total_cost_7d'] >= 5) & (df['total_sales_7d'] == 0)
definition8 = (df['ORDER_1m'] >= 8) & (df['total_cost_7d'] >= 10) & (df['total_sales_7d'] == 0)

# Combine all the conditions
conditions = definition1 | definition2 | definition3 | definition4 | definition5 | definition6 | definition7 | definition8
filtered_df = df[conditions]

# Determine which definition each row matches
def get_matching_definitions(row):
    definitions = []
    if definition1[row.name]: definitions.append("定义一")
    if definition2[row.name]: definitions.append("定义二")
    if definition3[row.name]: definitions.append("定义三")
    if definition4[row.name]: definitions.append("定义四")
    if definition5[row.name]: definitions.append("定义五")
    if definition6[row.name]: definitions.append("定义六")
    if definition7[row.name]: definitions.append("定义七")
    if definition8[row.name]: definitions.append("定义八")
    return ','.join(definitions)

filtered_df['definition'] = filtered_df.apply(get_matching_definitions, axis=1)

# Prepare the output DataFrame
output_df = filtered_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'definition']]

# Save the filtered DataFrame to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_UK_2024-07-15.csv'
output_df.to_csv(output_path, index=False)

print(f"Filtered data has been saved to {output_path}")