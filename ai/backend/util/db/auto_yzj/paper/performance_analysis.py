# filename: performance_analysis.py

import pandas as pd

# Load the dataset
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/预处理.csv"
output_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/提问策略/手动_ASIN_优质商品投放_v1_1_LAPASA_IT_2024-07-15.csv"
data = pd.read_csv(file_path)

# Define the conditions and corresponding price increases
def condition_one(row):
    return (0 < row['ACOS_7d'] <= 0.1) and (0 < row['ACOS_30d'] <= 0.1) and \
           (row['ORDER_1m'] >= 2) and (0 < row['ACOS_3d'] <= 0.2)

def condition_two(row):
    return (0 < row['ACOS_7d'] <= 0.1) and (0.1 < row['ACOS_30d'] <= 0.24) and \
           (row['ORDER_1m'] >= 2) and (0 < row['ACOS_3d'] <= 0.2)

def condition_three(row):
    return (0.1 < row['ACOS_7d'] <= 0.2) and (0 < row['ACOS_30d'] <= 0.1) and \
           (row['ORDER_1m'] >= 2) and (0 < row['ACOS_3d'] <= 0.2)

def condition_four(row):
    return (0.1 < row['ACOS_7d'] <= 0.2) and (0.1 < row['ACOS_30d'] <= 0.24) and \
           (row['ORDER_1m'] >= 2) and (0 < row['ACOS_3d'] <= 0.2)

def condition_five(row):
    return (0.2 < row['ACOS_7d'] <= 0.24) and (0 < row['ACOS_30d'] <= 0.1) and \
           (row['ORDER_1m'] >= 2) and (0 < row['ACOS_3d'] <= 0.2)

def condition_six(row):
    return (0.2 < row['ACOS_7d'] <= 0.24) and (0.1 < row['ACOS_30d'] <= 0.24) and \
           (row['ORDER_1m'] >= 2) and (0 < row['ACOS_3d'] <= 0.2)

# Apply conditions and update bids
data['New_keywordBid'] = data['keywordBid']
data['IncreaseAmount'] = 0.0
data['Reason'] = ""

for i, row in data.iterrows():
    if condition_one(row):
        data.at[i, 'New_keywordBid'] += 0.05
        data.at[i, 'IncreaseAmount'] = 0.05
        data.at[i, 'Reason'] = "Condition One"
    elif condition_two(row):
        data.at[i, 'New_keywordBid'] += 0.03
        data.at[i, 'IncreaseAmount'] = 0.03
        data.at[i, 'Reason'] = "Condition Two"
    elif condition_three(row):
        data.at[i, 'New_keywordBid'] += 0.04
        data.at[i, 'IncreaseAmount'] = 0.04
        data.at[i, 'Reason'] = "Condition Three"
    elif condition_four(row):
        data.at[i, 'New_keywordBid'] += 0.02
        data.at[i, 'IncreaseAmount'] = 0.02
        data.at[i, 'Reason'] = "Condition Four"
    elif condition_five(row):
        data.at[i, 'New_keywordBid'] += 0.02
        data.at[i, 'IncreaseAmount'] = 0.02
        data.at[i, 'Reason'] = "Condition Five"
    elif condition_six(row):
        data.at[i, 'New_keywordBid'] += 0.01
        data.at[i, 'IncreaseAmount'] = 0.01
        data.at[i, 'Reason'] = "Condition Six"

# Select required columns and save to CSV
columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_yesterday', 'total_clicks_yesterday', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'IncreaseAmount', 'Reason']
output_data = data[columns]
output_data.to_csv(output_path, index=False)

print("CSV file has been created successfully!")