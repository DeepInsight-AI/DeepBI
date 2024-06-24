# filename: find_click_anomalies.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
df = pd.read_csv(file_path)

# 计算近7天和近30天的日均点击量
df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

# 定义异常描述函数
def anomaly_description(row):
    changes = []
    if row['avg_clicks_7d'] > 0:
        if abs(row['clicks_yesterday'] - row['avg_clicks_7d']) / row['avg_clicks_7d'] > 0.3:
            changes.append("7-day click change > 30%")
    if row['avg_clicks_30d'] > 0:
        if abs(row['clicks_yesterday'] - row['avg_clicks_30d']) / row['avg_clicks_30d'] > 0.3:
            changes.append("30-day click change > 30%")
    return ', '.join(changes) if changes else None

# 判断异常现象
df['anomaly_description'] = df.apply(anomaly_description, axis=1)

# 筛选出存在异常的广告活动
anomalies_df = df[df['anomaly_description'].notnull()][[
    'campaignId', 'campaignName', 'placementClassification',
    'anomaly_description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d'
]]

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\广告位_点击量异常_FR_2024-05-18.csv'
anomalies_df.to_csv(output_file_path, index=False)

print(f"Anomalies saved to {output_file_path}")