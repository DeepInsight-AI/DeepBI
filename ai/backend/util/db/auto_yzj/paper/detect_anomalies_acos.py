# filename: detect_anomalies_acos.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv"
df = pd.read_csv(file_path)

# 定义异常检测函数
def detect_acos_anomalies(row):
    anomalies = []
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']

    if pd.isna(acos_yesterday) or acos_yesterday == 0:
        if acos_7d < 0.25:
            anomalies.append('昨天无销售额，近7天ACOS较好')
        if acos_7d < 0.20:
            anomalies.append('昨天无销售额，近7天ACOS极好')
        if acos_30d < 0.25:
            anomalies.append('昨天无销售额，近30天ACOS较好')
        if acos_30d < 0.20:
            anomalies.append('昨天无销售额，近30天ACOS极好')
    else:
        if acos_7d != 0 and abs(acos_yesterday - acos_7d) / acos_7d > 0.30:
            anomalies.append('昨天ACOS相对近7天平均ACOS波动超过30%')
        if acos_30d != 0 and abs(acos_yesterday - acos_30d) / acos_30d > 0.30:
            anomalies.append('昨天ACOS相对近30天平均ACOS波动超过30%')

    return anomalies

# 应用异常检测函数
df['Anomalies'] = df.apply(detect_acos_anomalies, axis=1)
anomalies_df = df[df['Anomalies'].apply(len) > 0]

# 构造输出的DataFrame
output_df = anomalies_df[['campaignId', 'campaignName', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 'Anomalies']]
output_df = output_df.explode('Anomalies')
output_df.rename(columns={'campaignId': '异常广告活动ID', 'campaignName': '异常广告活动', 'Anomalies': '异常现象'}, inplace=True)

# 保存到新的CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\异常检测_广告活动_ACOS异常_v1_0_LAPASA_FR_2024-07-12.csv"
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"异常检测结果已保存到 {output_file_path}")