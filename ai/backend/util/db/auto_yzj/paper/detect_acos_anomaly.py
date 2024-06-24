# filename: detect_acos_anomaly.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
df = pd.read_csv(file_path)

# 提取需要的字段
columns_needed = ['campaignId', 'campaignName', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d']
data = df[columns_needed]

# 检测ACOS异常
anomalies = []
for index, row in data.iterrows():
    campaignId = row['campaignId']
    campaignName = row['campaignName']
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    
    # 若昨天ACOS为空（NaN），仍需进行检测
    if pd.isna(acos_yesterday):
        if acos_7d < 0.25 or acos_30d < 0.25:
            description = '昨天无销售额，近7天或近30天ACOS较好'
        if acos_7d < 0.20 or acos_30d < 0.20:
            description = '昨天无销售额，近7天或近30天ACOS极好'
        anomalies.append([
            campaignId, campaignName, description, acos_yesterday, acos_7d, acos_30d
        ])
        continue
    
    # 计算ACOS的相对变化百分比
    acos_change_7d = abs(acos_yesterday - acos_7d) / acos_7d if acos_7d != 0 else float('inf')
    acos_change_30d = abs(acos_yesterday - acos_30d) / acos_30d if acos_30d != 0 else float('inf')
    
    # 若昨日ACOS值超过近7天或近30天ACOS的30%，为ACOS异常
    if acos_change_7d > 0.30:
        description = '昨天ACOS相对近7天变化超过30%'
        anomalies.append([
            campaignId, campaignName, description, acos_yesterday, acos_7d, acos_30d
        ])
    
    if acos_change_30d > 0.30:
        description = '昨天ACOS相对近30天变化超过30%'
        anomalies.append([
            campaignId, campaignName, description, acos_yesterday, acos_7d, acos_30d
        ])

# 将异常数据输出到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\广告活动_ACOS异常_IT_2024-06-08.csv'
anomalies_df = pd.DataFrame(anomalies, columns=['campaignId', 'Campaign Name', 'Anomaly Description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d'])
anomalies_df.to_csv(output_file_path, index=False)

print(f"Detected anomalies have been saved to {output_file_path}")