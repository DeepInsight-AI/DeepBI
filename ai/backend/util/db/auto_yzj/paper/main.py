# filename: main.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\预处理.csv'
data = pd.read_csv(file_path)

# 申请新的列保存新的出价
data['New Bid'] = data['keywordBid']
data['提价原因'] = ''

# 定义提价策略
def apply_bid_increase(row):
    ACOS_7d, ACOS_30d, orders_1m = row['ACOS_7d'], row['ACOS_30d'], row['ORDER_1m']
    
    if 0 < ACOS_7d <= 0.2 and 0 < ACOS_30d <= 0.2 and orders_1m >= 2:
        row['New Bid'] += 0.05
        row['提价原因'] = '定义一'
    elif 0 < ACOS_7d <= 0.2 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
        row['New Bid'] += 0.03
        row['提价原因'] = '定义二'
    elif 0.2 < ACOS_7d <= 0.24 and ACOS_30d <= 0.2 and orders_1m >= 2:
        row['New Bid'] += 0.04
        row['提价原因'] = '定义三'
    elif 0.2 < ACOS_7d <= 0.24 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
        row['New Bid'] += 0.02
        row['提价原因'] = '定义四'
    elif 0.24 < ACOS_7d <= 0.27 and ACOS_30d < 0.2 and orders_1m >= 2:
        row['New Bid'] += 0.02
        row['提价原因'] = '定义五'
    elif 0.24 < ACOS_7d <= 0.27 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
        row['New Bid'] += 0.01
        row['提价原因'] = '定义六'
    return row

# 应用提价策略
data = data.apply(apply_bid_increase, axis=1)

# 筛选出提价的关键词
filtered_data = data[data['提价原因'] != '']

# 选择需要的列
output_data = filtered_data[[
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
    'New Bid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 'ACOS_30d', 
    'ORDER_1m', 'New Bid', '提价原因'
]]

# 保存到新的CSV文件中
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\提问策略\手动_优质关键词_v1_1_LAPASA_FR_2024-07-03.csv'
output_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("关键词提价策略执行完毕，结果已保存到指定文件。")