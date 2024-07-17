# filename: 优质商品投放提价策略.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义提价策略函数
def apply_bid_increase(row):
    increase = 0
    reason = ""
    
    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        increase = 0.05
        reason = "定义一"
    elif 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        increase = 0.03
        reason = "定义二"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        increase = 0.04
        reason = "定义三"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        increase = 0.02
        reason = "定义四"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        increase = 0.02
        reason = "定义五"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        increase = 0.01
        reason = "定义六"
    
    return pd.Series([row['keywordBid'] + increase, increase, reason])

# 应用提价策略并生成新列
df[['New_keywordBid', 'Increase', 'Reason']] = df.apply(apply_bid_increase, axis=1)

# 筛选出提价的商品投放
result_df = df[df['Increase'] > 0]

# 选择需要输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName',
    'matchType', 'keywordBid', 'New_keywordBid', 'targeting',
    'total_cost_7d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d',
    'ORDER_1m', 'Increase', 'Reason'
]
result_df = result_df[output_columns]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_IT_2024-07-03.csv'
result_df.to_csv(output_file_path, index=False)

print(f"Result has been saved to {output_file_path}")