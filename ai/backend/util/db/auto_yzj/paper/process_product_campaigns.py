# filename: process_product_campaigns.py
import pandas as pd

# Step 1: Load the dataset
dataset_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(dataset_path)

# Step 2: Identify poor performing campaigns
conditions = [
    {
        "name": "定义一",
        "filter": (df["ACOS_7d"] > 0.24) & (df["ACOS_7d"] <= 0.5) & (df["ACOS_30d"] > 0) & (df["ACOS_30d"] <= 0.5),
        "new_bid_calculation": lambda row: row["keywordBid"] / ((row["ACOS_7d"] - 0.24) / 0.24 + 1)
    },
    {
        "name": "定义二",
        "filter": (df["ACOS_7d"] > 0.5) & (df["ACOS_30d"] <= 0.36),
        "new_bid_calculation": lambda row: row["keywordBid"] / ((row["ACOS_7d"] - 0.24) / 0.24 + 1)
    },
    {
        "name": "定义三",
        "filter": (df["total_clicks_7d"] >= 10) & (df["total_sales14d_7d"] == 0) & (df["ACOS_30d"] <= 0.36),
        "new_bid_calculation": lambda row: row["keywordBid"] - 0.04
    },
    {
        "name": "定义四",
        "filter": (df["total_clicks_7d"] > 10) & (df["total_sales14d_7d"] == 0) & (df["ACOS_30d"] > 0.5),
        "new_bid_calculation": lambda row: "关闭"
    },
    {
        "name": "定义五",
        "filter": (df["ACOS_7d"] > 0.5) & (df["ACOS_30d"] > 0.36),
        "new_bid_calculation": lambda row: "关闭"
    },
    {
        "name": "定义六",
        "filter": (df["total_sales14d_30d"] == 0) & (df["total_cost_30d"] >= 5),
        "new_bid_calculation": lambda row: "关闭"
    },
    {
        "name": "定义七",
        "filter": (df["total_sales14d_30d"] == 0) & (df["total_clicks_30d"] >= 15) & (df["total_clicks_7d"] > 0),
        "new_bid_calculation": lambda row: "关闭"
    }
]

poor_campaigns = []
for condition in conditions:
    filtered_df = df[condition["filter"]].copy()
    record_count = filtered_df.shape[0]
    print(f"Record count for {condition['name']}: {record_count}")
    if record_count > 0:
        filtered_df["New_keywordBid"] = filtered_df.apply(condition["new_bid_calculation"], axis=1)
        filtered_df["操作原因"] = condition["name"]
        print(f"Columns after applying {condition['name']}:")
        print(filtered_df.columns)
        poor_campaigns.append(filtered_df)

if poor_campaigns:
    result_df = pd.concat(poor_campaigns)
    print("Columns in the final concatenated DataFrame:")
    print(result_df.columns)
else:
    result_df = pd.DataFrame()

# Ensure all necessary columns are present
required_columns = [
    "keyword", "keywordId", "campaignName", "adGroupName", "matchType", "keywordBid", "New_keywordBid", "targeting",
    "total_cost_yesterday", "total_clicks_yesterday", "total_cost_7d", "total_sales14d_7d", "total_cost_30d",
    "ACOS_7d", "ACOS_30d", "total_clicks_30d", "操作原因"
]

# Identify missing columns
missing_columns = [col for col in required_columns if col not in result_df.columns]
if missing_columns:
    print("Missing columns:")
    print(missing_columns)
else:
    output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_OutdoorMaster_IT_2024-07-09.csv"
    result_df.to_csv(output_path, columns=required_columns, index=False)
    print(f"Results saved to {output_path}")