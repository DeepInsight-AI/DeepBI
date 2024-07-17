# filename: manual_ad_search_term_optimization.py

import pandas as pd

# Load the dataset
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv"
df = pd.read_csv(file_path)

# Definitions
conditions = [
    ((df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5), "定义一"),
    ((df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8), "定义二"),
    ((df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0), "定义三"),
    ((df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3), "定义四"),
    ((df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5), "定义五"),
    ((df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0), "定义六")
]

# Apply conditions and add reason
reasons = []
for condition, reason in conditions:
    df_matched = df[condition]
    df.loc[condition, 'reason'] = reason
    reasons.append(df_matched)

# Combine all reasons into a single DataFrame
final_df = pd.concat(reasons)

# Select necessary columns
final_df = final_df[[
    'campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 
    'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]]

# Save the result to a new CSV file
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_ASIN_劣质搜索词_v1_1_IT_2024-06-27.csv"
final_df.to_csv(output_path, index=False)

print("Processing complete. The result has been saved to:", output_path)