# filename: quality_keywords.py

import pandas as pd
from datetime import date

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义筛选条件并提价
def classify_keywords(row):
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    order_1m = row['ORDER_1m']
    bid = row['keywordBid']
    reason = ''

    if 0 < acos_7d <= 0.1 and 0 < acos_30d <= 0.1 and order_1m >= 2:
        new_bid = bid + 0.05
        reason = '定义一'
    elif 0 < acos_7d <= 0.1 and 0.1 < acos_30d <= 0.24 and order_1m >= 2:
        new_bid = bid + 0.03
        reason = '定义二'
    elif 0.1 < acos_7d <= 0.2 and acos_30d <= 0.1 and order_1m >= 2:
        new_bid = bid + 0.04
        reason = '定义三'
    elif 0.1 < acos_7d <= 0.2 and 0.1 < acos_30d <= 0.24 and order_1m >= 2:
        new_bid = bid + 0.02
        reason = '定义四'
    elif 0.2 < acos_7d <= 0.24 and acos_30d <= 0.1 and order_1m >= 2:
        new_bid = bid + 0.02
        reason = '定义五'
    elif 0.2 < acos_7d <= 0.24 and 0.1 < acos_30d <= 0.24 and order_1m >= 2:
        new_bid = bid + 0.01
        reason = '定义六'
    else:
        new_bid = bid

    return new_bid, reason

# 筛选关键词并更新出价和原因
data['newBid'], data['reason'] = zip(*data.apply(classify_keywords, axis=1))

# 筛选出有提价的关键词
qualified_data = data[data['reason'] != '']

# 添加额外的字段
qualified_data['date'] = date.today()

# 选择需要的字段
columns = [
    'date', 'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
    'keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_7d', 'ACOS_7d',
    'ACOS_30d', 'ORDER_1m', 'newBid', 'reason'
]
final_data = qualified_data[columns]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\优质关键词_FR_2024-5-28.csv'
final_data.to_csv(output_path, index=False)

print('Process completed and output saved to', output_path)