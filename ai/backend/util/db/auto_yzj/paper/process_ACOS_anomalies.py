# filename: process_acos_anomalies.py

import pandas as pd
import numpy as np

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
df = pd.read_csv(file_path)

# Extract relevant columns
df_relevant = df[['campaignId', 'campaignName', 'placementClassification', 
                  'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d']]

# Function to detect anomalies
def detect_anomalies(row):
    anomalies = []
    yesterday_acos = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    
    if pd.isna(yesterday_acos):
        if acos_7d < 0.25 or acos_30d < 0.25:
            anomalies.append("昨天无销售额，近7天ACOS极好" if acos_7d < 0.25 else "昨天无销售额，近30天ACOS极好")
        elif acos_7d < 0.20 or acos_30d < 0.20:
            anomalies.append("昨天无销售额，近7天ACOS优异" if acos_7d < 0.20 else "昨天无销售额，近30天ACOS优异")
    else:
        if abs(yesterday_acos - acos_7d) / acos_7d > 0.30:
            anomalies.append("昨天ACOS值相较近7天ACOS波动超过30%")
        if abs(yesterday_acos - acos_30d) / acos_30d > 0.30:
            anomalies.append("昨天ACOS值相较近30天ACOS波动超过30%")
    
    return ", ".join(anomalies)

# Apply the anomaly detection function
df_relevant['Anomaly Description'] = df_relevant.apply(detect_anomalies, axis=1)

# Filter out rows without anomalies
df_anomalies = df_relevant[df_relevant['Anomaly Description'] != ""]

# Saving to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_ACOS异常_v1_0_LAPASA_FR_2024-07-11.csv'
df_anomalies.to_csv(output_path, index=False)
print(f"Anomalies saved to {output_path}")