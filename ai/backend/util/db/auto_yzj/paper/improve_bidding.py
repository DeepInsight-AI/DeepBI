# filename: improve_bidding.py

import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# Initialize new columns
data['New_keywordBid'] = pd.NA
data['Bid_Increase'] = pd.NA
data['理由'] = pd.NA

# Define conditions and corresponding bid increases
def update_bids(row):
    conditions = [
        (row['ACOS_7d'] > 0) & (row['ACOS_7d'] <= 0.1) & 
        (row['ACOS_30d'] > 0) & (row['ACOS_30d'] <= 0.1) & 
        (row['ORDER_1m'] >= 2),
        
        (row['ACOS_7d'] > 0) & (row['ACOS_7d'] <= 0.1) & 
        (row['ACOS_30d'] > 0.1) & (row['ACOS_30d'] <= 0.24) & 
        (row['ORDER_1m'] >= 2),
        
        (row['ACOS_7d'] > 0.1) & (row['ACOS_7d'] <= 0.2) & 
        (row['ACOS_30d'] <= 0.1) & 
        (row['ORDER_1m'] >= 2),
        
        (row['ACOS_7d'] > 0.1) & (row['ACOS_7d'] <= 0.2) & 
        (row['ACOS_30d'] > 0.1) & (row['ACOS_30d'] <= 0.24) & 
        (row['ORDER_1m'] >= 2),
        
        (row['ACOS_7d'] > 0.2) & (row['ACOS_7d'] <= 0.24) & 
        (row['ACOS_30d'] <= 0.1) & 
        (row['ORDER_1m'] >= 2),
        
        (row['ACOS_7d'] > 0.2) & (row['ACOS_7d'] <= 0.24) & 
        (row['ACOS_30d'] > 0.1) & (row['ACOS_30d'] <= 0.24) & 
        (row['ORDER_1m'] >= 2)
    ]
    bid_increases = [0.05, 0.03, 0.04, 0.02, 0.02, 0.01]
    reasons = [
        "定义一：最近7天和一个月的平均ACOS值在0-0.1且最近一个月订单数>=2",
        "定义二：最近7天的平均ACOS值在0-0.1且最近一个月的平均ACOS值在0.1-0.24且最近一个月订单数>=2",
        "定义三：最近7天的平均ACOS值在0.1-0.2且最近一个月ACOS值<=0.1且最近一个月订单数>=2",
        "定义四：最近7天的平均ACOS值在0.1-0.2且最近一个月ACOS值在0.1-0.24且最近一个月订单数>=2",
        "定义五：最近7天的平均ACOS值在0.2-0.24且最近一个月ACOS值<=0.1且最近一个月订单数>=2",
        "定义六：最近7天的平均ACOS值在0.2-0.24且最近一个月ACOS值在0.1-0.24且最近一个月订单数>=2"
    ]
    
    for i, condition in enumerate(conditions):
        if condition:
            row['New_keywordBid'] = row['keywordBid'] + bid_increases[i]
            row['Bid_Increase'] = bid_increases[i]
            row['理由'] = reasons[i]
            return row
        
    return row

# Apply bidding strategy to the dataset
data = data.apply(update_bids, axis=1)

# Filter rows where bid was updated
result = data.dropna(subset=['New_keywordBid'])

# Save the result to a new CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_DELOMO_FR_2024-07-09.csv'
result.to_csv(output_path, index=False, encoding='utf-8')

print(f"处理完毕，结果已保存到 {output_path}")