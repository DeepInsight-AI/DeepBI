# filename: ad_click_anomaly_detection.py

import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
data = pd.read_csv(file_path)

# 计算日均点击量
data['avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['avg_clicks_30d'] = data['total_clicks_30d'] / 30

# 定义异常阈值
threshold = 0.30

# 识别点击量波动异常
def check_anomaly(row):
    clicks_yesterday = row['clicks_yesterday']
    avg_clicks_7d = row['avg_clicks_7d']
    avg_clicks_30d = row['avg_clicks_30d']
    
    anomaly_desc = []
    
    if avg_clicks_7d != 0 and abs(clicks_yesterday - avg_clicks_7d) / avg_clicks_7d > threshold:
        anomaly_desc.append(f"昨天点击量比近7天日均点击量变化超过30%（实际值: {clicks_yesterday}，近7天日均: {avg_clicks_7d}）")
        
    if avg_clicks_30d != 0 and abs(clicks_yesterday - avg_clicks_30d) / avg_clicks_30d > threshold:
        anomaly_desc.append(f"昨天点击量比近30天日均点击量变化超过30%（实际值: {clicks_yesterday}，近30天日均: {avg_clicks_30d}）")
        
    return ', '.join(anomaly_desc)

data['anomaly_desc'] = data.apply(check_anomaly, axis=1)

# 筛选出存在异常的记录
anomalies = data[data['anomaly_desc'] != '']

# 选择需要输出的列
output_columns = ['campaignId', 'campaignName', 'placementClassification', 'anomaly_desc', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']
anomalies = anomalies[output_columns]

# 输出到新的 CSV 文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_点击量异常_v1_0_LAPASA_FR_2024-07-11.csv'
anomalies.to_csv(output_file, index=False, encoding='utf-8-sig')

print("异常检测完成，结果已保存至:", output_file)