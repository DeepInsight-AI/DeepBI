# filename: cpc_clicks_anomaly_detection.py

import pandas as pd

# 定义文件路径
data_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\异常检测_投放词_CPC高但clicks少1_v1_0_LAPASA_UK_2024-07-12.csv'

# 读取数据
df = pd.read_csv(data_file_path)

# 计算日均点击量
df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

# 条件判断
condition1 = (df['CPC_yesterday'] > df['CPC_7d'] * 1.3) | (df['CPC_yesterday'] > df['CPC_30d'] * 1.3)
condition2 = (df['clicks_yesterday'] < df['avg_clicks_7d'] * 0.7) | (df['clicks_yesterday'] < df['avg_clicks_30d'] * 0.7)

# 确定异常数据
anomaly_df = df[condition1 & condition2 & df['CPC_yesterday'].notna()]

# 添加异常说明
anomaly_df['Anomaly Description'] = 'CPC高但clicks少'

# 选择必要的列
result_df = anomaly_df[['campaignName', 'adGroupName', 'targeting', 'matchType', 'Anomaly Description',
                        'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d', 'CPC_yesterday', 'CPC_7d', 'CPC_30d']]

# 输出结果到CSV文件
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("异常数据已输出到:", output_file_path)