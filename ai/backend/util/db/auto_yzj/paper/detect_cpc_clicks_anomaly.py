# filename: detect_cpc_clicks_anomaly.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv'
data = pd.read_csv(file_path)

# 过滤CPC不为空的数据
data = data.dropna(subset=['CPC_yesterday', 'CPC_7d', 'CPC_30d'])

# 计算近7天和近30天的日均点击量
data['avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['avg_clicks_30d'] = data['total_clicks_30d'] / 30

# 定义异常条件
def detect_anomaly(row):
    cpc_yesterday = row['CPC_yesterday']
    cpc_7d = row['CPC_7d']
    cpc_30d = row['CPC_30d']
    clicks_yesterday = row['clicks_yesterday']
    avg_clicks_7d = row['avg_clicks_7d']
    avg_clicks_30d = row['avg_clicks_30d']

    condition1 = (cpc_yesterday > 1.3 * cpc_7d) and (clicks_yesterday < 0.7 * avg_clicks_7d)
    condition2 = (cpc_yesterday > 1.3 * cpc_30d) and (clicks_yesterday < 0.7 * avg_clicks_30d)

    if condition1 or condition2:
        return True
    return False

data['Anomaly'] = data.apply(detect_anomaly, axis=1)

# 筛选异常数据
anomalies = data[data['Anomaly']]

# 选取需要的列
output_columns = [
    'campaignName', 'adGroupName', 'targeting', 'matchType', 'clicks_yesterday',
    'avg_clicks_7d', 'avg_clicks_30d', 'CPC_yesterday', 'CPC_7d', 'CPC_30d'
]
anomalies['Anomaly Description'] = 'CPC高但clicks少'
anomalies = anomalies[output_columns + ['Anomaly Description']]

# 保存结果到指定的csv文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\异常检测_投放词_CPC高但clicks少1_v1_0_LAPASA_FR_2024-07-11.csv'
anomalies.to_csv(output_file_path, index=False)
print(f'异常数据已保存到 {output_file_path}')