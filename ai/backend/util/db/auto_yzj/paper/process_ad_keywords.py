# filename: process_ad_keywords.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义增加竞价的函数
def adjust_bid(row):
    if 0 < row['ACOS_7d'] < 0.24:
        if row['ACOS_30d'] > 0.5:
            row['New_keywordBid'] = row['keywordBid'] + 0.01
            row['Adjustment'] = 0.01
            row['Reason'] = 'definition 1'
        if row['ORDER_1m'] > 0.5:
            row['New_keywordBid'] = row['keywordBid'] + 0.02
            row['Adjustment'] = 0.02
            row['Reason'] = 'definition 2'
    if 0.1 < row['ACOS_7d'] < 0.24 and 0 < row['ORDER_1m'] < 0.24:
        row['New_keywordBid'] = row['keywordBid'] + 0.03
        row['Adjustment'] = 0.03
        row['Reason'] = 'definition 3'
    if 0 < row['ACOS_7d'] < 0.1 and 0 < row['ORDER_1m'] < 0.24:
        row['New_keywordBid'] = row['keywordBid'] + 0.05
        row['Adjustment'] = 0.05
        row['Reason'] = 'definition 4'
    return row

# 进行调整
data['New_keywordBid'] = data['keywordBid']
data['Adjustment'] = 0
data['Reason'] = ''

data = data.apply(adjust_bid, axis=1)

# 筛选出调整后的关键词
result = data[(data['Adjustment'] > 0)][['campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New_keywordBid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'Adjustment', 'Reason']]

# 保存结果到新的CSV文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_v1_1_ES_2024-06-21.csv'
result.to_csv(output_file, index=False)

print("调整结果已保存至:", output_file)