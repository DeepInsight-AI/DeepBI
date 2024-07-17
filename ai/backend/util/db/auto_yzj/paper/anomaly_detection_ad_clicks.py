# filename: anomaly_detection_ad_clicks.py
import pandas as pd

# 定义文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\异常检测_广告位_点击量异常_v1_0_LAPASA_FR_2024_07_14.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 计算日均点击量
df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

# 绘制异常检查条件
def check_anomaly(row):
    yesterday_clicks = row['clicks_yesterday']
    avg_clicks_7d = row['avg_clicks_7d']
    avg_clicks_30d = row['avg_clicks_30d']
    
    desc = []
    if avg_clicks_7d != 0 and abs(yesterday_clicks - avg_clicks_7d) / avg_clicks_7d > 0.30:
        desc.append('近7天日均点击量波动异常')
    if avg_clicks_30d != 0 and abs(yesterday_clicks - avg_clicks_30d) / avg_clicks_30d > 0.30:
        desc.append('近30天日均点击量波动异常')
    
    return ', '.join(desc)

# 检查异常并保存结果
df['Anomaly Description'] = df.apply(check_anomaly, axis=1)
anomalies = df[df['Anomaly Description'] != '']

result = anomalies[['campaignId', 'campaignName', 'placementClassification', 'Anomaly Description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']]
result.to_csv(output_file, index=False)

print(f'输出到: {output_file}')