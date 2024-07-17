# filename: 优质商品投放策略.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义提价规则的函数
def update_bid(row):
    increase = 0
    reason = ""
    
    if (0 < row['ACOS_7d'] <= 0.1) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        increase = 0.05
        reason = "定义一"
    elif (0 < row['ACOS_7d'] <= 0.1) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        increase = 0.03
        reason = "定义二"
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        increase = 0.04
        reason = "定义三"
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        increase = 0.02
        reason = "定义四"
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        increase = 0.02
        reason = "定义五"
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        increase = 0.01
        reason = "定义六"
    
    return increase, reason

# 应用提价规则
df['提价'], df['提价原因'] = zip(*df.apply(update_bid, axis=1))
df['New_keywordBid'] = df['keywordBid'] + df['提价']

# 选择需要输出的列
output_df = df[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 'targeting', 
                'total_cost_7d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', '提价', '提价原因']]

# 输出结果
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_UK_2024-07-09.csv'
output_df.to_csv(output_file_path, index=False)

print("数据处理完成，结果已保存到:", output_file_path)