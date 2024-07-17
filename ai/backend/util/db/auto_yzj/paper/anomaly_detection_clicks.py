# filename: anomaly_detection_clicks.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
data = pd.read_csv(file_path)

# 计算近7天和近30天的日均点击量
data['avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['avg_clicks_30d'] = data['total_clicks_30d'] / 30

# 判断异常：点击量波动超过30%
def detect_anomaly(row):
    if row['avg_clicks_7d'] == 0 or row['avg_clicks_30d'] == 0:
        return False
    condition_7d = abs(row['clicks_yesterday'] - row['avg_clicks_7d']) / row['avg_clicks_7d'] > 0.3
    condition_30d = abs(row['clicks_yesterday'] - row['avg_clicks_30d']) / row['avg_clicks_30d'] > 0.3
    return condition_7d or condition_30d

anomalies = data[data.apply(detect_anomaly, axis=1)]

# 异常现象描述
def get_anomaly_description(row):
    if row['avg_clicks_7d'] == 0 or row['avg_clicks_30d'] == 0:
        return ''
    condition_7d = abs(row['clicks_yesterday'] - row['avg_clicks_7d']) / row['avg_clicks_7d'] > 0.3
    condition_30d = abs(row['clicks_yesterday'] - row['avg_clicks_30d']) / row['avg_clicks_30d'] > 0.3
    if condition_7d or condition_30d:
        return 'Click throughput fluctuation'
    return ''

anomalies['anomaly_description'] = anomalies.apply(get_anomaly_description, axis=1)

# 保存结果到CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_点击量异常_v1_0_LAPASA_DE_2024-07-14.csv'
anomalies[['campaignId', 'campaignName', 'placementClassification', 'anomaly_description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']].to_csv(output_file_path, index=False)

print(f"Anomalies saved to {output_file_path}")