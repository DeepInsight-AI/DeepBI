# filename: anomaly_detection.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\投放词\\预处理1.csv"
data = pd.read_csv(file_path)

# 定义日期
today = pd.Timestamp("2024-05-18")

# 计算7天每单平均点击次数和30天每单平均点击次数
data['avg_clicks_per_order_7d'] = data['total_clicks_7d'] / data['total_purchases7d_7d']
data['avg_clicks_per_order_30d'] = data['total_clicks_30d'] / data['total_purchases7d_30d']

# 初始化异常列表
anomalies = []

for index, row in data.iterrows():
    # 检查ACOS波动异常
    acos_yesterday = row['ACOS_yesterday']
    if pd.notnull(acos_yesterday):
        acos_7d = row['ACOS_7d']
        acos_30d = row['ACOS_30d']
        if ((abs(acos_yesterday - acos_7d) / acos_7d > 0.30) or (abs(acos_yesterday - acos_30d) / acos_30d > 0.30)):
            anomalies.append({
                "CampaignName": row['campaignName'],
                "adGroupName": row['adGroupName'],
                "advertisedSku": row['targeting'],
                "Anomaly Description": "ACOS波动异常",
                "ACOS_yesterday": acos_yesterday,
                "近七天ACOS": acos_7d,
                "近30天ACOS": acos_30d,
                "clicks_yesterday": row['clicks_yesterday'],
                "近七天每单平均点击次数": row['avg_clicks_per_order_7d'],
                "近30天每单平均点击次数": row['avg_clicks_per_order_30d'],
            })
    
    # 检查足够点击无销售异常
    if pd.isnull(acos_yesterday):
        if (row['clicks_yesterday'] > row['avg_clicks_per_order_7d']) and (row['purchases7d_yesterday'] == 0):
            anomalies.append({
                "CampaignName": row['campaignName'],
                "adGroupName": row['adGroupName'],
                "advertisedSku": row['targeting'],
                "Anomaly Description": "昨天点击量足够但无销售",
                "ACOS_yesterday": acos_yesterday,
                "近七天ACOS": row['ACOS_7d'],
                "近30天ACOS": row['ACOS_30d'],
                "clicks_yesterday": row['clicks_yesterday'],
                "近七天每单平均点击次数": row['avg_clicks_per_order_7d'],
                "近30天每单平均点击次数": row['avg_clicks_per_order_30d'],
            })

# 转成DataFrame并保存到CSV
anomaly_df = pd.DataFrame(anomalies)
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\投放词\\提问策略\\异常检测_投放词_点击量足够但ACOS异常1_v1_0_LAPASA_IT_2024-07-15.csv"
anomaly_df.to_csv(output_path, index=False)

print("异常检测完成并保存到CSV文件。")