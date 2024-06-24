# filename: process_ACOS_anomalies.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
df = pd.read_csv(file_path)

# 今天是2024年5月18日
# 定义异常条件
def detect_anomalies(row):
    yesterday_acos = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    
    anomalies = []
    # 检查ACOS为NaN的情况
    if pd.isna(yesterday_acos) or yesterday_acos == 0:
        if acos_7d < 0.25:
            anomalies.append("昨天无销售额，近7天ACOS极好")
        if acos_30d < 0.20:
            anomalies.append("昨天无销售额，近30天ACOS极好")
    
    # 检查ACOS波动
    if not pd.isna(yesterday_acos):
        if acos_7d > 0:
            if abs(yesterday_acos - acos_7d) / acos_7d > 0.30:
                anomalies.append("昨天ACOS与近7天ACOS波动超过30%")
        if acos_30d > 0:
            if abs(yesterday_acos - acos_30d) / acos_30d > 0.30:
                anomalies.append("昨天ACOS与近30天ACOS波动超过30%")

    return anomalies

# 查找异常广告活动
result = []
for idx, row in df.iterrows():
    anomalies = detect_anomalies(row)
    if anomalies:
        for anomaly in anomalies:
            result.append({
                'campaignId': row['campaignId'],
                'campaignName': row['campaignName'],
                'Anomaly Description': anomaly,
                'ACOS_yesterday': row['ACOS_yesterday'],
                'ACOS_7d': row['ACOS_7d'],
                'ACOS_30d': row['ACOS_30d']
            })

# 转为DataFrame
anomalies_df = pd.DataFrame(result)

# 输出至CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\广告活动_ACOS异常_ES_2024-06-12.csv'
anomalies_df.to_csv(output_path, index=False)

print(f"异常检测结果已保存至 {output_path}")