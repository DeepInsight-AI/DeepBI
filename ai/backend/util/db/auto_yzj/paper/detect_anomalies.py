# filename: detect_anomalies.py

import pandas as pd
import numpy as np

# 读取CSV数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv"
data = pd.read_csv(file_path)

# 计算7天和30天的每单平均点击次数
data['avg_clicks_per_purchase_7d'] = data.apply(
    lambda row: row['total_clicks_7d'] / row['total_purchases7d_7d'] if row['total_purchases7d_7d'] > 0 else np.nan,
    axis=1)
data['avg_clicks_per_purchase_30d'] = data.apply(
    lambda row: row['total_clicks_30d'] / row['total_purchases7d_30d'] if row['total_purchases7d_30d'] > 0 else np.nan, 
    axis=1)

# 检查ACOS波动异常
data['acos_change_7d'] = data.apply(
    lambda row: (row['ACOS_yesterday'] - row['ACOS_7d']) / row['ACOS_7d'] if row['ACOS_7d'] > 0 else np.nan, axis=1)
data['acos_change_30d'] = data.apply(
    lambda row: (row['ACOS_yesterday'] - row['ACOS_30d']) / row['ACOS_30d'] if row['ACOS_30d'] > 0 else np.nan, axis=1)

# 检查异常
def check_anomalies(row):
    anomalies = []
    
    # 足够点击无销售
    if pd.isna(row['ACOS_yesterday']) and row['clicks_yesterday'] > row['avg_clicks_per_purchase_7d'] and row['sales14d_yesterday'] == 0:
        anomalies.append("昨天点击量足够但无销售")
    
    # ACOS波动异常
    if abs(row['acos_change_7d']) > 0.30:
        anomalies.append("最近7天ACOS波动超过30%")
        
    if abs(row['acos_change_30d']) > 0.30:
        anomalies.append("最近30天ACOS波动超过30%")
    
    return '|'.join(anomalies)

data['anomalies'] = data.apply(check_anomalies, axis=1)
anomalies_data = data[data['anomalies'] != '']

# 筛选并输出到CSV文件
output_columns = [
    'campaignName', 'adGroupName', 'advertisedSku', 'anomalies',
    'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 'clicks_yesterday',
    'avg_clicks_per_purchase_7d', 'avg_clicks_per_purchase_30d'
]

output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\异常检测_商品_点击量足够但ACOS异常1_v1_0_LAPASA_IT_2024-07-15.csv"
anomalies_data.to_csv(output_file_path, columns=output_columns, index=False)

print(f"Anomalies detected and stored in {output_file_path}")