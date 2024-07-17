# filename: click_anomaly_detection.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
data = pd.read_csv(file_path, encoding='utf-8')

# 定义异常检测函数
def detect_anomalies(row):
    anomaly_description = []
    
    # 计算近7天和近30天的日均点击量
    if row['total_clicks_7d'] != 0:
        avg_clicks_7d = row['total_clicks_7d'] / 7
    else:
        avg_clicks_7d = 0
        
    if row['total_clicks_30d'] != 0:
        avg_clicks_30d = row['total_clicks_30d'] / 30
    else:
        avg_clicks_30d = 0
    
    # 昨天的点击量
    clicks_yesterday = row['clicks_yesterday']
    
    # 检查波动异常
    if avg_clicks_7d != 0:
        change_7d = abs(clicks_yesterday - avg_clicks_7d) / avg_clicks_7d
        if change_7d > 0.3:
            anomaly_description.append(
                f'昨日点击量与近7天日均点击量波动超过30%: {change_7d:.2%}')
    
    if avg_clicks_30d != 0:
        change_30d = abs(clicks_yesterday - avg_clicks_30d) / avg_clicks_30d
        if change_30d > 0.3:
            anomaly_description.append(
                f'昨日点击量与近30天日均点击量波动超过30%: {change_30d:.2%}')
        
    return ", ".join(anomaly_description)

# 应用异常检测
data['Anomaly Description'] = data.apply(detect_anomalies, axis=1)

# 过滤出存在异常的广告活动
anomalies = data[data['Anomaly Description'] != '']

# 选择需要输出的列
output_columns = [
    'campaignId', 'campaignName', 'placementClassification', 
    'Anomaly Description', 'clicks_yesterday', 
    'total_clicks_7d', 'total_clicks_30d'
]

# 准备输出的DataFrame
output_data = anomalies[output_columns].copy()

# 计算近七天与近三十天的日均点击量以便输出
output_data['avg_clicks_7d'] = output_data['total_clicks_7d'] / 7
output_data['avg_clicks_30d'] = output_data['total_clicks_30d'] / 30

# 删除原总点击量列
output_data = output_data.drop(columns=['total_clicks_7d', 'total_clicks_30d'])

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_点击量异常_v1_0_LAPASA_FR_2024-07-12.csv'
output_data.to_csv(output_file_path, index=False, encoding='utf-8')

print("异常检测完成并保存在文件中。")