# filename: acos_anomaly_detection.py

import pandas as pd
import numpy as np

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
df = pd.read_csv(file_path)

# 填充“昨天”的日期
today = '2024-05-18'
yesterday = '2024-05-17'

# 异常检测函数
def check_anomalies(row):
    anomalies = []

    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']

    # 销售额为0检查
    if pd.isna(acos_yesterday):
        if acos_7d < 25 or acos_30d < 25:
            if acos_7d < 20 or acos_30d < 20:
                anomalies.append("昨天无销售额，ACOS极好")
            else:
                anomalies.append("昨天无销售额，ACOS较好")

    # ACOS波动检查
    if pd.notna(acos_yesterday):
        if acos_7d > 0 and abs((acos_yesterday - acos_7d) / acos_7d) > 0.3:
            anomalies.append("昨天ACOS值变化超过30%（相较于近七天ACOS）")
        if acos_30d > 0 and abs((acos_yesterday - acos_30d) / acos_30d) > 0.3:
            anomalies.append("昨天ACOS值变化超过30%（相较于近30天ACOS）")

    return anomalies

# 处理数据
df['Anomalies'] = df.apply(check_anomalies, axis=1)

# 过滤出存在异常的记录
anomalies_df = df[df['Anomalies'].map(len) > 0]

# 展开异常记录
expanded_anomalies = anomalies_df.explode('Anomalies')

# 重命名列名
expanded_anomalies.rename(columns={
    'campaignId': 'Campaign ID',
    'campaignName': 'Campaign Name',
    'placementClassification': 'Placement Classification',
    'ACOS_yesterday': 'ACOS Yesterday',
    'ACOS_7d': 'ACOS 7d',
    'ACOS_30d': 'ACOS 30d',
    'Anomalies': 'Anomaly Description'
}, inplace=True)

# 设置输出 CSV 文件路径
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\广告位_ACOS异常_ES_2024-06-10.csv'

# 将异常记录保存到输出 CSV 文件
expanded_anomalies.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f'异常检测已完成，结果保存在 {output_file_path}')