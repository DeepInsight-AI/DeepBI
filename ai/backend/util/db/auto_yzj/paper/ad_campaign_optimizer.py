# filename: ad_campaign_optimizer.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义根据条件提价函数
def adjust_bid(row):
    reason = ""
    increase = 0

    # 定义 1
    if (0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2):
        increase = 0.05
        reason = "定义一"

    # 定义 2
    elif (0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2):
        increase = 0.03
        reason = "定义二"

    # 定义 3
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2):
        increase = 0.04
        reason = "定义三"

    # 定义 4
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2):
        increase = 0.02
        reason = "定义四"

    # 定义 5
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2):
        increase = 0.02
        reason = "定义五"

    # 定义 6
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2):
        increase = 0.01
        reason = "定义六"

    new_bid = row['keywordBid'] + increase
    
    return pd.Series([new_bid, increase, reason])

# 过滤数据并添加新列
data[['New_Bid', 'Bid_Increase', 'Reason']] = data.apply(adjust_bid, axis=1)

# 过滤出有提价的记录
filtered_data = data[data['Bid_Increase'] > 0]

# 选择所需要的列
columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_Bid',
           'targeting', 'total_cost_yesterday', 'total_clicks_yesterday', 'ACOS_7d', 'ACOS_30d', 
           'ORDER_1m', 'Bid_Increase', 'Reason']

result_data = filtered_data[columns]

# 导出结果到CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_IT_2024-06-27.csv'
result_data.to_csv(output_file_path, index=False)

print(f"Result saved to {output_file_path}")