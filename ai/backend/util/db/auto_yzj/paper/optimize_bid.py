# filename: optimize_bid.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path, encoding='utf-8')

# 定义新的列来存储提价信息和原因
data['New_keywordBid'] = data['keywordBid']
data['提价原因'] = ''  # Default Reason
data['提价幅度'] = 0  # Default Increment

# 定义筛选条件和提价策略
conditions = [
    (data['ACOS_7d'].between(0, 0.1) & data['ACOS_30d'].between(0, 0.1) & (data['ORDER_1m'] >= 2), 0.05, "定义一"),
    (data['ACOS_7d'].between(0, 0.1) & data['ACOS_30d'].between(0.1, 0.24) & (data['ORDER_1m'] >= 2), 0.03, "定义二"),
    (data['ACOS_7d'].between(0.1, 0.2) & data['ACOS_30d'].between(0, 0.1) & (data['ORDER_1m'] >= 2), 0.04, "定义三"),
    (data['ACOS_7d'].between(0.1, 0.2) & data['ACOS_30d'].between(0.1, 0.24) & (data['ORDER_1m'] >= 2), 0.02, "定义四"),
    (data['ACOS_7d'].between(0.2, 0.24) & data['ACOS_30d'].between(0, 0.1) & (data['ORDER_1m'] >= 2), 0.02, "定义五"),
    (data['ACOS_7d'].between(0.2, 0.24) & data['ACOS_30d'].between(0.1, 0.24) & (data['ORDER_1m'] >= 2), 0.01, "定义六")
]

# 应用条件来更新竞价和设置提价原因
for cond, increment, reason in conditions:
    data.loc[cond, 'New_keywordBid'] += increment
    data.loc[cond, '提价原因'] = reason
    data.loc[cond, '提价幅度'] = increment

# 准备输出列
output_cols = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
    'New_keywordBid', 'targeting', 'total_cost_7d', 'total_clicks_7d', 'ACOS_7d', 
    'ACOS_30d', 'ORDER_1m', '提价幅度', '提价原因'
]

# 输出结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_ES_2024-07-09.csv'
data.to_csv(output_file_path, columns=output_cols, index=False, encoding='utf-8')

print("提价策略应用完成，结果已保存到新的CSV文件中。")