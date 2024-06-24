# filename: increase_bid_keywords.py

import pandas as pd

# Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# Function to adjust bids based on the defined criteria
def adjust_bids(data):
    results = []

    for index, row in data.iterrows():
        campaignName = row['campaignName']
        adGroupName = row['adGroupName']
        keyword = row['keyword']
        keywordBid = row['keywordBid']
        ACOS_30d = row['ACOS_30d']
        ACOS_7d = row['ACOS_7d']
        clicks_7d = row['total_clicks_7d']

        new_bid = keywordBid
        adjustment_reason = ""
        
        # Apply rules based on ACOS values
        if 0 < ACOS_7d < 0.24 and ACOS_30d > 0.5:
            new_bid += 0.01
            adjustment_reason = "Definition 1"
            # Check for additional criteria within Definition 1
            if row['total_clicks_30d'] > 0:
                new_bid += 0.01
                adjustment_reason = "Definition 2"
        elif 0.1 < ACOS_7d < 0.24 and 0 < ACOS_30d < 0.24:
            new_bid += 0.03
            adjustment_reason = "Definition 3"
        elif 0 < ACOS_7d < 0.1 and 0 < ACOS_30d < 0.24:
            new_bid += 0.05
            adjustment_reason = "Definition 4"

        if adjustment_reason:
            results.append([
                campaignName, adGroupName, keyword, keywordBid, new_bid, ACOS_30d, ACOS_7d, clicks_7d, new_bid - keywordBid, adjustment_reason
            ])
    
    return results

# Process and adjust bids
adjusted_data = adjust_bids(data)

# Convert the results to a DataFrame
results_df = pd.DataFrame(adjusted_data, columns=[
    'campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New_keywordBid', 'ACOS_30d', 'ACOS_7d', 'clicks_7d', 'Bid_Increase', 'Adjustment_Reason'
])

# Save the adjusted data to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_v1_1_ES_2024-06-20.csv'
results_df.to_csv(output_file_path, index=False)

print("Bids adjusted and results saved to:", output_file_path)