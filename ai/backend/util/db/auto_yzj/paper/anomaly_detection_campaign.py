# filename: anomaly_detection_campaign.py

import pandas as pd

file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\异常检测_投放词_CPC高但clicks少1_v1_0_LAPASA_ES_2024-07-10.csv"

# 读取数据
df = pd.read_csv(file_path)

# 涉及到的字段
fields = ["campaignName", "adGroupName", "targeting", "matchType", "clicks_yesterday", 
          "total_clicks_30d", "total_clicks_7d", "CPC_yesterday", 
          "CPC_7d", "CPC_30d"]

# 只保留涉及到的字段
df = df[fields]

# 计算日均点击量
df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

# 筛选CPC非空的数据
df = df.dropna(subset=['CPC_yesterday', 'CPC_7d', 'CPC_30d'])

# 计算CPC和点击量的变化百分比
df['CPC_7d_change'] = (df['CPC_yesterday'] - df['CPC_7d']) / df['CPC_7d']
df['CPC_30d_change'] = (df['CPC_yesterday'] - df['CPC_30d']) / df['CPC_30d']
df['clicks_7d_change'] = (df['clicks_yesterday'] - df['avg_clicks_7d']) / df['avg_clicks_7d']
df['clicks_30d_change'] = (df['clicks_yesterday'] - df['avg_clicks_30d']) / df['avg_clicks_30d']

# 筛选符合条件的异常数据
anomalies = df[((df['CPC_7d_change'] > 0.3) & (df['clicks_7d_change'] < -0.3)) | 
               ((df['CPC_30d_change'] > 0.3) & (df['clicks_30d_change'] < -0.3))]

anomalies['Anomaly Description'] = 'CPC高但clicks少'

# 输出结果
output_fields = ["campaignName", "adGroupName", "targeting", "matchType", "Anomaly Description",
                 "clicks_yesterday", "avg_clicks_7d", "avg_clicks_30d",
                 "CPC_yesterday", "CPC_7d", "CPC_30d"]

anomalies[output_fields].to_csv(output_file_path, index=False)

print(f"Anomalies saved to {output_file_path}")