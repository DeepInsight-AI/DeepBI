# filename: preprocess_search_terms.py

import pandas as pd

# Step 1: Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# Step 2: Filter the data according to the given definitions

# Create a list to store the rows that meet the conditions
results = []

# Iterate over the dataframe rows
for index, row in df.iterrows():
    reasons = []
    
    # Check definition 1
    if 0.24 < row['ACOS_30d'] < 0.36 and row['ORDER_1m'] <= 5:
        reasons.append("定义一")
    
    # Check definition 2
    if row['ACOS_30d'] >= 0.36 and row['ORDER_1m'] <= 8:
        reasons.append("定义二")
    
    # Check definition 3
    if row['total_clicks_30d'] > 13 and row['ORDER_1m'] == 0:
        reasons.append("定义三")
    
    # Check definition 4
    if 0.24 < row['ACOS_7d'] < 0.36 and row['ORDER_7d'] <= 3:
        reasons.append("定义四")
    
    # Check definition 5
    if row['ACOS_7d'] >= 0.36 and row['ORDER_7d'] <= 5:
        reasons.append("定义五")
    
    # Check definition 6
    if row['total_clicks_7d'] > 10 and row['ORDER_7d'] == 0:
        reasons.append("定义六")
    
    # If any definition is satisfied, add the row to the results
    if reasons:
        results.append({
            'Campaign Name': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'week_clicks': row['total_clicks_7d'],
            'week_acos': row['ACOS_7d'],
            'week_orders': row['ORDER_7d'],
            'sum_clicks': row['total_clicks_30d'],
            'month_orders': row['ORDER_1m'],
            'month_acos': row['ACOS_30d'],
            'searchTerm': row['searchTerm'],
            'reason': "、".join(reasons)
        })

# Convert the results to a DataFrame
result_df = pd.DataFrame(results)

# Step 3: Save the result to a new CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_ES_2024-07-09.csv'
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"Result saved to {output_path}")