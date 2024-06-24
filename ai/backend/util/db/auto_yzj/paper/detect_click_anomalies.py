# filename: detect_click_anomalies.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
df = pd.read_csv(file_path)

# 确保数据集包含必要的字段
required_columns = [
    'campaignId', 'campaignName', 'placementClassification', 'clicks_yesterday',
    'total_clicks_7d', 'total_clicks_30d'
]
df = df[required_columns]

# 计算日均点击量，同时处理零值情况
df['avg_clicks_7d'] = df['total_clicks_7d'].apply(lambda x: x / 7 if x != 0 else 0)
df['avg_clicks_30d'] = df['total_clicks_30d'].apply(lambda x: x / 30 if x != 0 else 0)

# 定义异常检测函数
def detect_anomaly(row):
    clicks_yesterday = row['clicks_yesterday']
    avg_clicks_7d = row['avg_clicks_7d']
    avg_clicks_30d = row['avg_clicks_30d']
    
    anomaly_desc = []
    
    # 波动异常检查
    if avg_clicks_7d != 0 and (abs(clicks_yesterday - avg_clicks_7d) / avg_clicks_7d > 0.3):
        anomaly_desc.append("Clicks deviation from 7d average")
    
    if avg_clicks_30d != 0 and (abs(clicks_yesterday - avg_clicks_30d) / avg_clicks_30d > 0.3):
        anomaly_desc.append("Clicks deviation from 30d average")
    
    # 返回异常描述
    return ', '.join(anomaly_desc)

# 检查异常
df['Anomaly Description'] = df.apply(detect_anomaly, axis=1)

# 过滤有异常的广告活动
df_anomalies = df[df['Anomaly Description'] != '']

# 选择要保存的列
output_columns = [
    'campaignId', 'campaignName', 'placementClassification', 'Anomaly Description',
    'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d'
]

df_anomalies = df_anomalies[output_columns]

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\广告位_点击量异常_ES_2024-06-12.csv'
df_anomalies.to_csv(output_file_path, index=False)

print("Anomalies detection completed and saved.")