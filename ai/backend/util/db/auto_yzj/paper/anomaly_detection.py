# filename: anomaly_detection.py
import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv"
data = pd.read_csv(file_path, encoding='utf-8')

# 过滤CPC为空的记录
data = data.dropna(subset=['CPC_yesterday', 'CPC_7d', 'CPC_30d'])

# 计算近7天和近30天的日均点击量
data['点击量近7天日均'] = data['total_clicks_7d'] / 7
data['点击量近30天日均'] = data['total_clicks_30d'] / 30

def is_anomalous(row):
    cpc_yesterday = row['CPC_yesterday']
    cpc_7d = row['CPC_7d']
    cpc_30d = row['CPC_30d']
    clicks_yesterday = row['clicks_yesterday']
    avg_clicks_7d = row['点击量近7天日均']
    avg_clicks_30d = row['点击量近30天日均']

    # CPC升高超过30%的条件
    cpc_7d_increase = cpc_yesterday > cpc_7d * 1.3
    cpc_30d_increase = cpc_yesterday > cpc_30d * 1.3

    # 点击量下降超过30%的条件
    clicks_7d_decrease = clicks_yesterday < avg_clicks_7d * 0.7
    clicks_30d_decrease = clicks_yesterday < avg_clicks_30d * 0.7

    # 判断异常现象
    if (cpc_7d_increase and clicks_7d_decrease) or (cpc_30d_increase and clicks_30d_decrease):
        return True
    return False

# 筛选异常记录
anomalous_data = data[data.apply(is_anomalous, axis=1)]

# 添加异常现象描述
anomalous_data['Anomaly Description'] = 'CPC高但Clicks少'

# 输出到CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\投放词_CPC高但clicks少1_ES_2024-06-07.csv"
anomalous_data.to_csv(output_file_path, columns=[
    'campaignName', 'adGroupName', 'targeting', 'matchType', 'Anomaly Description',
    'clicks_yesterday', '点击量近7天日均', '点击量近30天日均', 'CPC_yesterday', 'CPC_7d', 'CPC_30d'
], index=False, encoding='utf-8')

print("分析已完成，结果已保存到CSV文件。")