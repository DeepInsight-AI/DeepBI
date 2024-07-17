# filename: detect_anomalies_fixed.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv'
df = pd.read_csv(file_path)

# 计算7天和30天每单平均点击次数
df['average_clicks_per_order_7d'] = df['total_clicks_7d'] / df['total_purchases7d_7d']
df['average_clicks_per_order_30d'] = df['total_clicks_30d'] / df['total_purchases7d_30d']

# 保留两位小数
df['average_clicks_per_order_7d'] = df['average_clicks_per_order_7d'].round(2)
df['average_clicks_per_order_30d'] = df['average_clicks_per_order_30d'].round(2)

# 异常条件
anomalies = []

for index, row in df.iterrows():
    anomaly_description = []
    
    if pd.isnull(row['ACOS_yesterday']):  # 检查ACOS为空的情况
        if row['total_purchases7d_7d'] != 0 and row['purchases7d_yesterday'] == 0:
            if row['clicks_yesterday'] > row['average_clicks_per_order_7d']:
                anomaly_description.append('昨天点击量足够但无销售 (近7天)')
        if row['total_purchases7d_30d'] != 0 and row['purchases7d_yesterday'] == 0:
            if row['clicks_yesterday'] > row['average_clicks_per_order_30d']:
                anomaly_description.append('昨天点击量足够但无销售 (近30天)')
    else:  # 检查ACOS波动
        if row['ACOS_yesterday'] > 1.3 * row['ACOS_7d'] or row['ACOS_yesterday'] < 0.7 * row['ACOS_7d']:
            anomaly_description.append('昨天ACOS相对近7天波动异常')
        if row['ACOS_yesterday'] > 1.3 * row['ACOS_30d'] or row['ACOS_yesterday'] < 0.7 * row['ACOS_30d']:
            anomaly_description.append('昨天ACOS相对近30天波动异常')
    
    if anomaly_description:
        anomalies.append([
            row['campaignName'], row['adGroupName'], row['advertisedSku'], "; ".join(anomaly_description),
            row['ACOS_yesterday'], row['ACOS_7d'], row['ACOS_30d'], row['clicks_yesterday'],
            row['average_clicks_per_order_7d'], row['average_clicks_per_order_30d']
        ])

# 构建异常检测的DataFrame
anomalies_df = pd.DataFrame(anomalies, columns=[
    'CampaignName', 'adGroupName', 'advertisedSku', 'Anomaly Description',
    'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 'clicks_yesterday',
    'average_clicks_per_order_7d', 'average_clicks_per_order_30d'
])

# 保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\异常检测_商品_点击量足够但ACOS异常1_v1_0_LAPASA_ES_2024-07-12.csv'
anomalies_df.to_csv(output_file_path, index=False)

print(f"Anomalies have been detected and saved to {output_file_path}")