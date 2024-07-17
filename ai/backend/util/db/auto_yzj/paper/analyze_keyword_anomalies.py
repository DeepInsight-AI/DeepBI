# filename: analyze_keyword_anomalies.py

import pandas as pd

# 读取CSV文件
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/异常定位检测/投放词/预处理1.csv'
df = pd.read_csv(file_path)

# 确保CPC字段不为空
df = df.dropna(subset=['CPC_yesterday', 'CPC_7d', 'CPC_30d'])

# 计算7天和30天的日均点击量
df['7d_avg_clicks'] = df['total_clicks_7d'] / 7
df['30d_avg_clicks'] = df['total_clicks_30d'] / 30

# 找出CPC异常且点击量下降的记录
anomalies = df[
    ((df['CPC_yesterday'] > df['CPC_7d'] * 1.3) & (df['clicks_yesterday'] < df['7d_avg_clicks'] * 0.7)) |
    ((df['CPC_yesterday'] > df['CPC_30d'] * 1.3) & (df['clicks_yesterday'] < df['30d_avg_clicks'] * 0.7))
]

# 添加异常现象的描述
anomalies['Anomaly Description'] = 'CPC高但clicks少'

# 选择并重命名需要的列
result = anomalies[[
    'campaignName', 
    'adGroupName', 
    'targeting', 
    'matchType', 
    'Anomaly Description',
    'clicks_yesterday',
    '7d_avg_clicks',
    '30d_avg_clicks',
    'CPC_yesterday',
    'CPC_7d',
    'CPC_30d'
]]

# 保存结果为CSV
output_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/异常定位检测/投放词/提问策略/异常检测_投放词_CPC高但clicks少1_v1_0_LAPASA_US_2024-07-12.csv'
result.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"数据已保存到 {output_path}")