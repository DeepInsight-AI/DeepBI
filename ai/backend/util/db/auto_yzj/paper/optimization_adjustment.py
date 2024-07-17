# filename: optimization_adjustment.py

import pandas as pd

# 读取CSV文件的数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义函数来计算新的竞价值和提价原因
def calculate_new_bid_and_reason(row):
    new_bid = row['keywordBid']
    reason = ""
    
    if (0 < row['ACOS_7d'] <= 0.1) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        new_bid += 0.05
        reason = "定义一：7天ACOS <= 0.1 & 30天ACOS <= 0.1 & 订单数 >= 2"
    elif (0 < row['ACOS_7d'] <= 0.1) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        new_bid += 0.03
        reason = "定义二：7天ACOS <= 0.1 & 30天ACOS 0.1-0.24 & 订单数 >= 2"
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        new_bid += 0.04
        reason = "定义三：7天ACOS 0.1-0.2 & 30天ACOS <= 0.1 & 订单数 >= 2"
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        new_bid += 0.02
        reason = "定义四：7天ACOS 0.1-0.2 & 30天ACOS 0.1-0.24 & 订单数 >= 2"
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        new_bid += 0.02
        reason = "定义五：7天ACOS 0.2-0.24 & 30天ACOS <= 0.1 & 订单数 >= 2"
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        new_bid += 0.01
        reason = "定义六：7天ACOS 0.2-0.24 & 30天ACOS 0.1-0.24 & 订单数 >= 2"
        
    return new_bid, reason

# 计算新的竞价值和提价原因
df[['New_keywordBid', '提价原因']] = df.apply(lambda row: calculate_new_bid_and_reason(row), axis=1, result_type="expand")

# 选取需要的列
output_df = df[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_7d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', '提价原因']]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_DE_2024-07-10.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Results have been saved to {output_file_path}")