# filename: detect_ACOS_anomalies.py

import pandas as pd
import numpy as np

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
df = pd.read_csv(file_path)

# 异常检测函数
def detect_anomalies(row):
    anomalies = []
    
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    
    # 昨天ACOS值为空，近7天或近30天ACOS较好或极好
    if pd.isna(acos_yesterday) or acos_yesterday == 0:
        if acos_7d < 0.25 or acos_30d < 0.25:
            anomalies.append("昨天无销售额，近7天ACOS较好" if acos_7d < 0.25 else "")
            anomalies.append("昨天无销售额，近30天ACOS较好" if acos_30d < 0.25 else "")
        if acos_7d < 0.20 or acos_30d < 0.20:
            anomalies.append("昨天无销售额，近7天ACOS极好" if acos_7d < 0.20 else "")
            anomalies.append("昨天无销售额，近30天ACOS极好" if acos_30d < 0.20 else "")
    
    # ACOS波动异常检测
    if not pd.isna(acos_yesterday) and acos_yesterday != 0:
        if abs((acos_yesterday - acos_7d) / acos_7d) > 0.3:
            anomalies.append("昨天ACOS相对于近七天均值的波动超过30%")
        if abs((acos_yesterday - acos_30d) / acos_30d) > 0.3:
            anomalies.append("昨天ACOS相对于近三十天均值的波动超过30%")
    
    return "; ".join(filter(None, anomalies))

# 进行检测并保存结果
df['Anomaly Description'] = df.apply(detect_anomalies, axis=1)
anomalies_df = df[df['Anomaly Description'] != ""]

# 选择需要的列
result_columns = ['campaignId', 'campaignName', 'Anomaly Description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d']
anomalies_df = anomalies_df[result_columns]

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\异常检测_广告活动_ACOS异常_v1_0_LAPASA_IT_2024-07-15.csv'
anomalies_df.to_csv(output_path, index=False)

print(f"异常检测结果已保存至: {output_path}")