# filename: process_ad_campaigns.py

import pandas as pd

# Define file paths
input_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_劣质广告活动_v1_1_ES_2024-06-14.csv"

# Load data
df = pd.read_csv(input_file_path)

# Add new columns for calculated fields
df["New_Budget"] = df["Budget"]
df["Reason"] = ""

# Apply definition 1
condition1 = (df["ACOS_7d"] > 0.24) & (df["ACOS_yesterday"] > 0.24) & (df["clicks_yesterday"] >= 10) & (df["ACOS_30d"] > df["country_avg_ACOS_1m"])
df.loc[condition1, "New_Budget"] = df["Budget"].apply(lambda x: max(8, x-5))
df.loc[condition1, "Reason"] += "Definition 1, "

# Apply definition 2
condition2 = (df["ACOS_7d"] > 0.24) & (df["ACOS_yesterday"] > 0.24) & (df["cost_yesterday"] > 0.8 * df["Budget"]) & (df["ACOS_30d"] > df["country_avg_ACOS_1m"])
df.loc[condition2, "New_Budget"] = df["Budget"].apply(lambda x: max(8, x-5))
df.loc[condition2, "Reason"] += "Definition 2, "

# Apply definition 3
condition3 = (df["ACOS_30d"] > 0.24) & (df["ACOS_30d"] > df["country_avg_ACOS_1m"]) & (df["total_sales14d_7d"] == 0) & (df["total_clicks_7d"] >= 15)
df.loc[condition3, "New_Budget"] = df["Budget"].apply(lambda x: max(5, x-5))
df.loc[condition3, "Reason"] += "Definition 3, "

# Apply definition 4
condition4 = (df["total_sales14d_30d"] == 0) & (df["total_clicks_30d"] >= 75)
df.loc[condition4, "New_Budget"] = "关闭"
df.loc[condition4, "Reason"] += "Definition 4, "

# Filtered data for output
output_columns = ["campaignId", "campaignName", "Budget", "New_Budget", "clicks_yesterday", "ACOS_yesterday", "ACOS_7d",
                  "total_clicks_7d", "total_sales14d_7d", "ACOS_30d", "total_clicks_30d", "total_sales14d_30d", 
                  "country_avg_ACOS_1m", "Reason"]
filtered_df = df.loc[df["Reason"] != "", output_columns]

# Save the result to a new CSV file
filtered_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"Processed data has been saved to {output_file_path}")