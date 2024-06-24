# filename: analyze_ad_acos.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
data = pd.read_csv(file_path)

# 昨天的日期假设是2024年5月18日
yesterday = '2024-05-17'

# 检查ACOS值的函数
def check_acos_anomaly(row):
    anomalies = []
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']

    # 如果昨天ACOS为空，查看近7天或近30天ACOS是否较好或极好
    if pd.isna(acos_yesterday):
        if acos_7d < 0.25 or acos_30d < 0.25:
            anomalies.append('昨天无销售额，近7天ACOS较好或较好（或近30天ACOS极好）')

    else:
        # 与近7天的ACOS比较
        if acos_7d != 0 and abs(acos_yesterday - acos_7d) / acos_7d > 0.30:
            anomalies.append('ACOS值相对近7天波动超过30%')

        # 与近30天的ACOS比较
        if acos_30d != 0 and abs(acos_yesterday - acos_30d) / acos_30d > 0.30:
            anomalies.append('ACOS值相对近30天波动超过30%')
    
    return anomalies

# 对每条记录进行异常检查
data['Anomaly Description'] = data.apply(lambda row: ', '.join(check_acos_anomaly(row)), axis=1)

# 过滤出异常的记录
anomalous_data = data[data['Anomaly Description'] != '']

# 保留所需的列
result = anomalous_data[['campaignId', 'campaignName', 'placementClassification', 'Anomaly Description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d']]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\广告位_ACOS异常_ES_2024-06-121.csv'
result.to_csv(output_file_path, index=False)

print("Analysis complete. Results saved to:", output_file_path)