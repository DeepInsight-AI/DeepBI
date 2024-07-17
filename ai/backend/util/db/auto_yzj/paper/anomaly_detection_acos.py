# filename: anomaly_detection_acos.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
df = pd.read_csv(file_path)

# 创建一个用于存储异常结果的DataFrame
anomaly_results = pd.DataFrame(columns=[
    'campaignId', 'Campaign Name', 'placementClassification', 'Anomaly Description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d'
])

# 当前日期假定为2024年5月18日，昨天为5月17日
def detect_anomalies(row):
    anomalies = []
    ACOS_yesterday = row['ACOS_yesterday']
    ACOS_7d = row['ACOS_7d']
    ACOS_30d = row['ACOS_30d']
    
    # 检查是否为空以及相应的异常
    if pd.isnull(ACOS_yesterday):
        if ACOS_7d < 25 or ACOS_30d < 25:
            anomalies.append('昨天无销售额，近7天ACOS较好')
        if ACOS_7d < 20 or ACOS_30d < 20:
            anomalies.append('昨天无销售额，近30天ACOS极好')

    # 检查ACOS波动异常
    if not pd.isnull(ACOS_yesterday) and not pd.isnull(ACOS_7d) and not pd.isnull(ACOS_30d):
        if abs(ACOS_yesterday - ACOS_7d) / ACOS_7d > 0.30:
            anomalies.append('昨天ACOS与近7天平均ACOS值相比波动超过30%')
        if abs(ACOS_yesterday - ACOS_30d) / ACOS_30d > 0.30:
            anomalies.append('昨天ACOS与近30天平均ACOS值相比波动超过30%')

    return anomalies

# 检测各行数据
for index, row in df.iterrows():
    detected_anomalies = detect_anomalies(row)
    if detected_anomalies:
        new_row = pd.DataFrame([{
            'campaignId': row['campaignId'],
            'Campaign Name': row['campaignName'],
            'placementClassification': row['placementClassification'],
            'Anomaly Description': '; '.join(detected_anomalies),
            'ACOS_yesterday': row['ACOS_yesterday'],
            'ACOS_7d': row['ACOS_7d'],
            'ACOS_30d': row['ACOS_30d']
        }])
        anomaly_results = pd.concat([anomaly_results, new_row], ignore_index=True)

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_ACOS异常_v1_0_LAPASA_IT_2024-07-15.csv'
anomaly_results.to_csv(output_file_path, index=False)

print("异常检测完成，结果已保存到:", output_file_path)