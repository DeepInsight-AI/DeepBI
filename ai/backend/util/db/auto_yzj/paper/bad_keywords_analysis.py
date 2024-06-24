# filename: bad_keywords_analysis.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
data = pd.read_csv(file_path)

# 筛选表现较差的关键词
bad_keywords = data[(data['total_clicks'] <= 12) & (data['total_sales'] == 0)]

# 提高竞价
bad_keywords['keywordBid'] = bad_keywords['keywordBid'] + 0.02

# 添加竞价原因
operation_reason = "总点击次数小于等于12且总销售额为0，提高竞价0.02"
bad_keywords['operation_reason'] = operation_reason

# 选择输出的列
output_columns = [
    'campaignName',
    'adGroupName',
    'keyword',
    'keywordId',
    'targeting',
    'matchtype',
    'total_clicks',
    'total_sales',
    'keywordBid',
    'operation_reason'
]

# 输出结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_ES_2024-06-07.csv'
bad_keywords[output_columns].to_csv(output_path, index=False)

print(f"Results have been saved to {output_path}")