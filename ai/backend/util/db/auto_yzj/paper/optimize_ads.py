# filename: optimize_ads.py

import pandas as pd

# Step 1: Load the Data
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(file_path)

# Step 2: Process Data
def get_new_bid(row):
    if (row['ACOS_7d'] > 0) & (row['ACOS_7d'] <= 0.1) & (row['ACOS_30d'] > 0) & (row['ACOS_30d'] <= 0.1) & (row['ORDER_1m'] >= 2) & (row['ACOS_3d'] > 0) & (row['ACOS_3d'] <= 0.2):
        return row['keywordBid'] + 0.05, "定义一"
    elif (row['ACOS_7d'] > 0) & (row['ACOS_7d'] <= 0.1) & (row['ACOS_30d'] > 0.1) & (row['ACOS_30d'] <= 0.24) & (row['ORDER_1m'] >= 2) & (row['ACOS_3d'] > 0) & (row['ACOS_3d'] <= 0.2):
        return row['keywordBid'] + 0.03, "定义二"
    elif (row['ACOS_7d'] > 0.1) & (row['ACOS_7d'] <= 0.2) & (row['ACOS_30d'] <= 0.1) & (row['ORDER_1m'] >= 2) & (row['ACOS_3d'] > 0) & (row['ACOS_3d'] <= 0.2):
        return row['keywordBid'] + 0.04, "定义三"
    elif (row['ACOS_7d'] > 0.1) & (row['ACOS_7d'] <= 0.2) & (row['ACOS_30d'] > 0.1) & (row['ACOS_30d'] <= 0.24) & (row['ORDER_1m'] >= 2) & (row['ACOS_3d'] > 0) & (row['ACOS_3d'] <= 0.2):
        return row['keywordBid'] + 0.02, "定义四"
    elif (row['ACOS_7d'] > 0.2) & (row['ACOS_7d'] <= 0.24) & (row['ACOS_30d'] <= 0.1) & (row['ORDER_1m'] >= 2) & (row['ACOS_3d'] > 0) & (row['ACOS_3d'] <= 0.2):
        return row['keywordBid'] + 0.02, "定义五"
    elif (row['ACOS_7d'] > 0.2) & (row['ACOS_7d'] <= 0.24) & (row['ACOS_30d'] > 0.1) & (row['ACOS_30d'] <= 0.24) & (row['ORDER_1m'] >= 2) & (row['ACOS_3d'] > 0) & (row['ACOS_3d'] <= 0.2):
        return row['keywordBid'] + 0.01, "定义六"
    else:
        return row['keywordBid'], None

df[['New_keywordBid', '原因']] = df.apply(get_new_bid, axis=1, result_type='expand')

# Filter rows with a reason
result_df = df[df['原因'].notnull()]

# Select and rename columns
output_columns = [
    'keyword',
    'keywordId',
    'campaignName',
    'adGroupName',
    'matchType',              # Assuming the column is named 'matchType'
    'keywordBid',
    'New_keywordBid',
    'targeting',
    'total_cost_30d',
    'total_clicks_30d',       # Assuming this is `clicks` as per user's query
    'ACOS_7d',                # 最近7天的平均ACOS值
    'ACOS_30d',               # 最近一个月的平均ACOS值
    'ORDER_1m',               # 最近一个月的订单数
    '原因'
]

output_df = result_df[output_columns]

# Step 4: Save the Output
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_UK_2024-07-15.csv"
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"Output saved to {output_file_path}")