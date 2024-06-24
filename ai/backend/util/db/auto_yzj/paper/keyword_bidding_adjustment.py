# filename: keyword_bidding_adjustment.py

import pandas as pd
from datetime import datetime

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
df = pd.read_csv(file_path)

# 添加提价和原因的列
df['提价金额'] = 0
df['提价原因'] = ''

# 定义筛选条件和执行提价
def adjust_bidding(row):
    if 0 < row['ACOS_7d'] <= 0.1:
        if 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            row['提价金额'] = 0.05
            row['提价原因'] = '定义一'
        elif 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            row['提价金额'] = 0.03
            row['提价原因'] = '定义二'
    
    elif 0.1 < row['ACOS_7d'] <= 0.2:
        if 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            row['提价金额'] = 0.04
            row['提价原因'] = '定义三'
        elif 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            row['提价金额'] = 0.02
            row['提价原因'] = '定义四'
    
    elif 0.2 < row['ACOS_7d'] <= 0.24:
        if 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            row['提价金额'] = 0.02
            row['提价原因'] = '定义五'
        elif 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            row['提价金额'] = 0.01
            row['提价原因'] = '定义六'
    
    return row

# 应用筛选条件和提价调整
df = df.apply(adjust_bidding, axis=1)

# 过滤出提价的关键词
df_filtered = df[df['提价金额'] > 0]

# 设置输出文件的路径和文件名
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_优质关键词_IT_' + datetime.now().strftime('%Y-%m-%d') + '.csv'

# 选择需要输出的列
output_columns = [
    'date', 'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 
    'ACOS_30d', 'ORDER_1m', '提价金额', '提价原因'
]

df_filtered['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
df_filtered = df_filtered[output_columns]

# 输出结果到CSV文件
df_filtered.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f'调整后的关键词已保存到: {output_file_path}')