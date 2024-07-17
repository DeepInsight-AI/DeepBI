# filename: click_anomaly_detection_final.py

import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
data = pd.read_csv(file_path)

# 计算近7天和近30天的日均点击量
data['avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['avg_clicks_30d'] = data['total_clicks_30d'] / 30

# 异常判断
def check_anomaly(row):
    anomalies = []
    if row['avg_clicks_7d'] != 0 and abs(row['clicks_yesterday'] - row['avg_clicks_7d']) / row['avg_clicks_7d'] > 0.3:
        anomalies.append("昨天的点击量相较于近7天日均点击量波动超过30%")
    if row['avg_clicks_30d'] != 0 and abs(row['clicks_yesterday'] - row['avg_clicks_30d']) / row['avg_clicks_30d'] > 0.3:
        anomalies.append("昨天的点击量相较于近30天日均点击量波动超过30%")
    return "; ".join(anomalies)

data['Anomaly Description'] = data.apply(check_anomaly, axis=1)

# 筛选出存在异常的广告活动
anomalies_data = data[data['Anomaly Description'] != '']

# 提取需要的字段
anomalies_data = anomalies_data[['campaignId', 'campaignName', 'placementClassification', 'Anomaly Description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_点击量异常_v1_0_LAPASA_ES_2024-07-09.csv'
anomalies_data.to_csv(output_file_path, index=False)

print("异常检测已完成，结果已保存至文件中。")