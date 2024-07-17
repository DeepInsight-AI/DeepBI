# filename: display_data.py
import pandas as pd

# Step 1: Load the dataset
dataset_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(dataset_path)

# Display the relevant data fields for further inspection
display_columns = [
    "keywordId", "keyword", "targeting", "matchType", "adGroupName",
    "campaignName", "ORDER_1m", "total_clicks_30d", "total_clicks_7d",
    "total_clicks_yesterday", "total_sales14d_30d", "total_sales14d_7d",
    "total_sales14d_yesterday", "total_cost_30d", "total_cost_7d",
    "total_cost_4d", "total_cost_yesterday", "ACOS_30d", "ACOS_7d",
    "ACOS_yesterday", "keywordBid"
]
# Display a sample of the data
print(df[display_columns].head(10))  # Display first 10 rows to inspect