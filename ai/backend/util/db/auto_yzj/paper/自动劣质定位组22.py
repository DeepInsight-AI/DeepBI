# filename: auto_keyword_optimization.py

import pandas as pd

# 1. Load data from CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 2. Define conditions
def update_bid(row):
    new_bid = row['keywordBid']
    reason = None
    if 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24:
        new_bid = new_bid - 0.03
        reason = "定义一"
    elif 0.24 < row['ACOS_7d'] < 0.5 and 0.24 < row['ACOS_30d'] < 0.5:
        new_bid = new_bid - 0.04
        reason = "定义二"
    elif row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0 and 0.24 < row['ACOS_30d'] < 0.5:
        new_bid = new_bid - 0.04
        reason = "定义三"
    elif 0.24 < row['ACOS_7d'] < 0.5 and row['ACOS_30d'] > 0.5:
        new_bid = new_bid - 0.05
        reason = "定义四"
    elif row['ACOS_7d'] > 0.5 and 0 < row['ACOS_30d'] < 0.24:
        new_bid = new_bid - 0.05
        reason = "定义五"
    elif row['ORDER_1m'] == 0 and row['total_clicks_30d'] > 13 and row['total_clicks_7d'] > 0:
        new_bid = '关闭'
        reason = "定义六"
    elif row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0 and row['ACOS_30d'] > 0.5:
        new_bid = '关闭'
        reason = "定义七"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.24:
        new_bid = '关闭'
        reason = "定义八"
    
    if reason:
        return pd.Series([new_bid, reason])
    else:
        return pd.Series([None, None])

# 3. Apply conditions to identify changes
data[['New Bid', 'Reason']] = data.apply(update_bid, axis=1)

# 4. Filter rows where changes are needed
filtered_data = data.dropna(subset=['New Bid'])

# 5. Output results to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_1_ES_2024-06-21.csv'
filtered_data.to_csv(output_file_path, index=False, columns=['campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New Bid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'Reason'])

print("Process completed and results saved.")