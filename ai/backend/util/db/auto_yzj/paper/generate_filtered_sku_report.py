# filename: generate_filtered_sku_report.py

import pandas as pd

# Load the CSV file
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\滞销品优化\\手动sp广告\\关闭SKU\\预处理.csv'
data = pd.read_csv(file_path)

# Define filters based on provided conditions
def condition_one(row):
    return row['ORDER_1m'] < 5 and row['ACOS_7d'] > 0.6 and row['total_clicks_7d'] > 13

def condition_two(row):
    return row['ORDER_1m'] < 5 and row['ACOS_30d'] > 0.6 and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 13

def condition_three(row):
    return row['ORDER_1m'] < 5 and row['ACOS_7d'] > 0.6 and row['ACOS_30d'] > 0.6

def condition_four(row):
    return row['total_clicks_30d'] > 50 and row['total_sales14d_30d'] == 0

def condition_five(row):
    return row['ORDER_1m'] < 5 and row['total_clicks_7d'] >= 19 and row['total_sales14d_7d'] == 0

def condition_six(row):
    return row['total_clicks_7d'] >= 30 and row['total_sales14d_7d'] == 0

# Apply filters
filtered_data = data[
    data.apply(lambda row: condition_one(row) or condition_two(row) or condition_three(row) or condition_four(row) or condition_five(row) or condition_six(row), axis=1)
]

# Add a "Definition" column to identify which definition is satisfied
def identify_definition(row):
    if condition_one(row):
        return "定义一"
    elif condition_two(row):
        return "定义二"
    elif condition_three(row):
        return "定义三"
    elif condition_four(row):
        return "定义四"
    elif condition_five(row):
        return "定义五"
    elif condition_six(row):
        return "定义六"
    else:
        return ""

filtered_data['满足的定义'] = filtered_data.apply(identify_definition, axis=1)

# Select the necessary columns
output_data = filtered_data[
    ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义']
]

# Save the results to a new CSV file
output_file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\滞销品优化\\手动sp广告\\关闭SKU\\提问策略\\手动_关闭SKU_v1_1_LAPASA_FR_2024-07-03.csv'
output_data.to_csv(output_file_path, index=False)

print("Filtered SKU data saved successfully.")