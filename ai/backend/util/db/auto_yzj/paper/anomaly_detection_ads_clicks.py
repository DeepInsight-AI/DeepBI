# filename: anomaly_detection_ads_clicks.py

import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
df = pd.read_csv(file_path, encoding='utf-8')

# 计算日均点击量
df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

# 定义异常检测函数
def detect_anomaly(row):
    clicks_yesterday = row['clicks_yesterday']
    avg_clicks_7d = row['avg_clicks_7d']
    avg_clicks_30d = row['avg_clicks_30d']

    anomaly_desc = []

    # 检查与近7天日均点击量的对比，避免除零
    if avg_clicks_7d == 0:
        anomaly_desc.append('avg_clicks_7d is zero')
    else:
        if abs(clicks_yesterday - avg_clicks_7d) / avg_clicks_7d > 0.3:
            anomaly_desc.append('clicks_yesterday vs. avg_clicks_7d > 30%')

    # 检查与近30天日均点击量的对比，避免除零
    if avg_clicks_30d == 0:
        anomaly_desc.append('avg_clicks_30d is zero')
    else:
        if abs(clicks_yesterday - avg_clicks_30d) / avg_clicks_30d > 0.3:
            anomaly_desc.append('clicks_yesterday vs. avg_clicks_30d > 30%')

    return ', '.join(anomaly_desc) if anomaly_desc else None

# 检测异常
df['Anomaly_Description'] = df.apply(detect_anomaly, axis=1)

# 过滤出存在异常的广告活动
anomalies = df[df['Anomaly_Description'].notnull()]

# 准备输出结果
output_columns = ['campaignId', 'campaignName', 'placementClassification', 'Anomaly_Description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']
output_df = anomalies[output_columns]

# 将结果保存到指定的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_点击量异常_v1_0_LAPASA_UK_2024-07-10.csv'
output_df.to_csv(output_path, index=False, encoding='utf-8')

print("异常检测结果已保存至:", output_path)