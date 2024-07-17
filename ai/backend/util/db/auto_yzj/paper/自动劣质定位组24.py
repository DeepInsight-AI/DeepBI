# filename: process_keywords.py
import pandas as pd

# Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# Initialize an empty list to store the results
results = []

# Process each row in the data
for index, row in data.iterrows():
    keyword_info = {
        "campaignName": row['campaignName'],
        "adGroupName": row['adGroupName'],
        "keyword": row['keyword'],
        "keywordBid": row['keywordBid'],
        "New Bid": row['keywordBid'],
        "ACOS_30d": row['ACOS_30d'],
        "ACOS_7d": row['ACOS_7d'],
        "clicks_7d": row['total_clicks_7d'],
        "Reason": ""
    }

    if 0.24 < row['ACOS_7d'] < 0.5:
        if 0 < row['ACOS_30d'] < 0.24:
            keyword_info["New Bid"] = row['keywordBid'] - 0.03
            keyword_info["Reason"] = "Definition 1"
        elif 0.24 < row['ACOS_30d'] < 0.5:
            keyword_info["New Bid"] = row['keywordBid'] - 0.04
            keyword_info["Reason"] = "Definition 2"
        elif row['ACOS_30d'] > 0.5:
            keyword_info["New Bid"] = row['keywordBid'] - 0.05
            keyword_info["Reason"] = "Definition 4"
    if (row['total_sales14d_7d'] == 0 and
        row['total_clicks_7d'] > 0 and
        0.24 < row['ACOS_30d'] < 0.5):
        keyword_info["New Bid"] = row['keywordBid'] - 0.04
        keyword_info["Reason"] += "Definition 3"
    if (row['total_sales14d_30d'] == 0 and
        row['total_clicks_30d'] > 13 and
        row['total_clicks_7d'] > 0):
        keyword_info["New Bid"] = "Close"
        keyword_info["Reason"] += "Definition 6"
    if (row['total_sales14d_7d'] == 0 and
        row['total_clicks_7d'] > 0 and
        row['ACOS_30d'] > 0.5):
        keyword_info["New Bid"] = "Close"
        keyword_info["Reason"] += "Definition 7"
    if (row['ACOS_7d'] > 0.5 and
        row['ACOS_30d'] > 0.24):
        keyword_info["New Bid"] = "Close"
        keyword_info["Reason"] += "Definition 8"

    if keyword_info["Reason"]:  # Only keep the rows where we have performed an action
        results.append(keyword_info)

# Create a DataFrame from the results
results_df = pd.DataFrame(results)

# Save the results to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_1_IT_2024-06-24.csv'
results_df.to_csv(output_file_path, index=False)

print("Process finished and results saved.")
