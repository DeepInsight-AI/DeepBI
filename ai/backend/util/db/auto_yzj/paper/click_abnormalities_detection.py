# filename: click_abnormalities_detection.py

import pandas as pd

# 替代路径为你的CSV文件的路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
data = pd.read_csv(file_path)

# 用 0 填充缺失值，以防止计算时出错
data.fillna(0, inplace=True)

# 计算近7天和近30天的日均点击量
data['avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['avg_clicks_30d'] = data['total_clicks_30d'] / 30

# 定义一个函数来判断点击量是否异常
def click_anomaly(row):
    clicks_yesterday = row['clicks_yesterday']
    avg_clicks_7d = row['avg_clicks_7d']
    avg_clicks_30d = row['avg_clicks_30d']

    # 避免除零错误
    if avg_clicks_7d == 0 and avg_clicks_30d == 0:
        return False

    anomaly_7d = abs(clicks_yesterday - avg_clicks_7d) / avg_clicks_7d > 0.3 if avg_clicks_7d != 0 else False
    anomaly_30d = abs(clicks_yesterday - avg_clicks_30d) / avg_clicks_30d > 0.3 if avg_clicks_30d != 0 else False

    return anomaly_7d or anomaly_30d

# 过滤出存在异常的广告活动
anomalies = data[data.apply(click_anomaly, axis=1)]

# 构造异常现象描述
def anomaly_description(row):
    desc = []
    if row['avg_clicks_7d'] != 0 and abs(row['clicks_yesterday'] - row['avg_clicks_7d']) / row['avg_clicks_7d'] > 0.3:
        desc.append(f"昨天的点击量 ({row['clicks_yesterday']}) 比近期7天平均点击量 ({row['avg_clicks_7d']:.2f}) 波动超过30%。")
    if row['avg_clicks_30d'] != 0 and abs(row['clicks_yesterday'] - row['avg_clicks_30d']) / row['avg_clicks_30d'] > 0.3:
        desc.append(f"昨天的点击量 ({row['clicks_yesterday']}) 比近期30天平均点击量 ({row['avg_clicks_30d']:.2f}) 波动超过30%。")
    return " ".join(desc)

anomalies['Anomaly Description'] = anomalies.apply(anomaly_description, axis=1)

# 选择需要的列
output_columns = ['campaignId', 'campaignName', 'placementClassification', 'Anomaly Description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']
output_data = anomalies[output_columns]

# 保存到新的 CSV 文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\广告位_点击量异常_ES_2024-06-07.csv'
output_data.to_csv(output_file_path, index=False)

print("Detection and output process completed successfully.")