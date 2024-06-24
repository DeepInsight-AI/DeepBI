# filename: process_and_output_data_final.py
import pandas as pd
import numpy as np

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv')

# 定义筛选条件
conditions = [
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) &
    (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) &
    (data['ORDER_1m'] >= 2),
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) &
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
    (data['ORDER_1m'] >= 2),
    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) &
    (data['ACOS_30d'] <= 0.1) &
    (data['ORDER_1m'] >= 2),
    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) &
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
    (data['ORDER_1m'] >= 2),
    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_30d'] <= 0.1) &
    (data['ORDER_1m'] >= 2),
    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
    (data['ORDER_1m'] >= 2)
]
choices = [0.05, 0.03, 0.04, 0.02, 0.02, 0.01]
data['new_bid'] = np.select(conditions, choices, default=0)

# 输出结果到CSV文件
output_data = data[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'targeting', 'total_cost_7d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'new_bid']]
output_data.columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', '匹配类型', '关键词出价', 'targeting', 'cost', 'clicks', '最近7天的平均ACOS值', '最近一个月的平均ACOS值', '最近一个月的订单数', '对该词进行提价多少以及提价原因']
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\优质关键词_FR_2024-5-28_deepseek.csv', index=False)

print("Data processing and output completed.")