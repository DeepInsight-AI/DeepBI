# filename: ad_performance_analysis.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义判断条件
conditions = [
    # 定义一
    {'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) & (data['ORDER_1m'] >= 2),
     'increase': 0.05, 'reason': '定义一'},
    # 定义二
    {'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & (data['ORDER_1m'] >= 2),
     'increase': 0.03, 'reason': '定义二'},
    # 定义三
    {'condition': (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) & (data['ORDER_1m'] >= 2),
     'increase': 0.04, 'reason': '定义三'},
    # 定义四
    {'condition': (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & (data['ORDER_1m'] >= 2),
     'increase': 0.02, 'reason': '定义四'},
    # 定义五
    {'condition': (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) & (data['ORDER_1m'] >= 2),
     'increase': 0.02, 'reason': '定义五'},
    # 定义六
    {'condition': (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & (data['ORDER_1m'] >= 2),
     'increase': 0.01, 'reason': '定义六'},
]

# 筛选符合条件的数据并计算新的出价
results = []
for cond in conditions:
    filtered_data = data[cond['condition']].copy()
    filtered_data['New_keywordBid'] = filtered_data['keywordBid'] + cond['increase']
    filtered_data['Increase'] = cond['increase']
    filtered_data['Reason'] = cond['reason']
    results.append(filtered_data)

final_results = pd.concat(results)

# 选择需要输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid',
    'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'Increase', 'Reason'
]

# 保存结果到CSV
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_DELOMO_IT_2024-07-09.csv'
final_results[output_columns].to_csv(output_file, index=False)

print(f"结果已保存到 {output_file}")