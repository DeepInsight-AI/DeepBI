# filename: detect_acos_anomalies.py

import pandas as pd
from datetime import datetime

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
df = pd.read_csv(file_path)

# 定义异常检测函数
def detect_anomalies(row):
    anomalies = []
    # 获取相关字段值
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    
    # 检查ACOS值是否为空
    if pd.isna(acos_yesterday):
        if acos_7d < 0.25 or acos_30d < 0.25:
            anomalies.append("昨日无销售额，近7天或近30天ACOS较好")
        if acos_7d < 0.20 or acos_30d < 0.20:
            anomalies.append("昨日无销售额，近7天或近30天ACOS极好")
    else:
        # 检查年波动异常
        if acos_yesterday > acos_7d * 1.30 or acos_yesterday < acos_7d * 0.70:
            anomalies.append("昨日ACOS相较于近7天的波动异常")
        if acos_yesterday > acos_30d * 1.30 or acos_yesterday < acos_30d * 0.70:
            anomalies.append("昨日ACOS相较于近30天的波动异常")

    return ", ".join(anomalies)

# 应用异常检测函数
df['Anomaly Description'] = df.apply(detect_anomalies, axis=1)

# 筛选出存在异常的记录
anomalies_df = df[df['Anomaly Description'] != '']

# 选择输出列
output_df = anomalies_df[['campaignId', 'campaignName', 'Anomaly Description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d']]

# 保存结果到指定CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\广告活动_ACOS异常_IT_2024-06-11.csv'
output_df.to_csv(output_file_path, index=False)

print("结果已保存至:", output_file_path)