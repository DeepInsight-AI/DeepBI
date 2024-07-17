# filename: click_anomaly_detect.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
data = pd.read_csv(file_path)

# 当前日期定义
today = pd.Timestamp('2024-05-18')

# 计算日均点击量
data['avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['avg_clicks_30d'] = data['total_clicks_30d'] / 30

# 定义异常检测函数
def detect_anomaly(row):
    anomalies = []
    if row['avg_clicks_7d'] > 0 and abs(row['clicks_yesterday'] - row['avg_clicks_7d']) / row['avg_clicks_7d'] > 0.3:
        anomalies.append('点击量与近七天日均点击量相比波动超过30%')
    if row['avg_clicks_30d'] > 0 and abs(row['clicks_yesterday'] - row['avg_clicks_30d']) / row['avg_clicks_30d'] > 0.3:
        anomalies.append('点击量与近30天日均点击量相比波动超过30%')
    return "; ".join(anomalies)

# 检测异常
data['Anomaly Description'] = data.apply(detect_anomaly, axis=1)

# 过滤异常的行
anomalies = data[data['Anomaly Description'] != '']

# 选择需要的列
anomalies = anomalies[['campaignId', 'campaignName', 'placementClassification', 
                       'Anomaly Description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_点击量异常_v1_0_LAPASA_IT_2024-07-15.csv'
anomalies.to_csv(output_file_path, index=False)

print("Anomaly detection completed. Results saved to", output_file_path)