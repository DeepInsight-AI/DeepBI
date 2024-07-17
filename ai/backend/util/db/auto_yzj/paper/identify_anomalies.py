# filename: identify_anomalies.py

import pandas as pd

# 读取数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\投放词\\预处理1.csv"
data = pd.read_csv(file_path)

# 计算7天和30天每单平均点击次数
data['average_clicks_per_order_7d'] = (data['total_clicks_7d'] / data['total_purchases7d_7d']).round(2)
data['average_clicks_per_order_30d'] = (data['total_clicks_30d'] / data['total_purchases7d_30d']).round(2)

# 定义异常列表
anomalies = []

# 异常判断
for index, row in data.iterrows():
    has_anomaly = False
    anomaly_description = []
    
    # 检查足够点击无销售异常
    if pd.isnull(row['ACOS_yesterday']):
        if (row['clicks_yesterday'] > row['average_clicks_per_order_7d'] or row['clicks_yesterday'] > row['average_clicks_per_order_30d']) and row['sales14d_yesterday'] == 0:
            has_anomaly = True
            anomaly_description.append("昨天点击量足够但无销售")

    # 检查ACOS值波动异常
    if 'ACOS_yesterday' in row and not pd.isnull(row['ACOS_yesterday']):
        if 'ACOS_7d' in row and not pd.isnull(row['ACOS_7d']):
            if abs(row['ACOS_yesterday'] - row['ACOS_7d']) / row['ACOS_7d'] > 0.3:
                has_anomaly = True
                anomaly_description.append("昨天ACOS与近7天ACOS波动超过30%")
        if 'ACOS_30d' in row and not pd.isnull(row['ACOS_30d']):
            if abs(row['ACOS_yesterday'] - row['ACOS_30d']) / row['ACOS_30d'] > 0.3:
                has_anomaly = True
                anomaly_description.append("昨天ACOS与近30天ACOS波动超过30%")
    
    if has_anomaly:
        anomalies.append({
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'advertisedSku': '',  # 没有advertisedSku字段，所以留空
            'Anomaly Description': "; ".join(anomaly_description),
            'ACOS_yesterday': row['ACOS_yesterday'],
            'ACOS_7d': row['ACOS_7d'],
            'ACOS_30d': row['ACOS_30d'],
            'clicks_yesterday': row['clicks_yesterday'],
            'average_clicks_per_order_7d': row['average_clicks_per_order_7d'],
            'average_clicks_per_order_30d': row['average_clicks_per_order_30d']
        })

# 转换结果为DataFrame
anomalies_df = pd.DataFrame(anomalies)

# 保存结果到CSV文件
output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\投放词\\提问策略\\异常检测_投放词_点击量足够但ACOS异常1_v1_0_LAPASA_DE_2024-07-12.csv"
anomalies_df.to_csv(output_file_path, index=False)

print(f"异常检测完成，结果已保存到: {output_file_path}")