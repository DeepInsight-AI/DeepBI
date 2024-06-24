# filename: detect_budget_anomalies.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
data = pd.read_csv(file_path)

# 计算日均花费
data['avg_cost_7d'] = data['total_cost_7d'] / 7
data['avg_cost_30d'] = data['total_cost_30d'] / 30

# 定义异常检测函数
def detect_anomalies(row):
    anomalies = []
    
    if row['cost_yesterday'] > row['campaignBudgetAmount'] and (row['avg_cost_7d'] < (row['campaignBudgetAmount'] * 0.5) and row['avg_cost_30d'] < (row['campaignBudgetAmount'] * 0.5)):
        anomalies.append('超出预算异常')
    
    # Avoid zero division
    if row['avg_cost_7d'] != 0 and abs(row['cost_yesterday'] - row['avg_cost_7d']) / row['avg_cost_7d'] > 0.3:
        anomalies.append('近7天波动异常')
    
    if row['avg_cost_30d'] != 0 and abs(row['cost_yesterday'] - row['avg_cost_30d']) / row['avg_cost_30d'] > 0.3:
        anomalies.append('近30天波动异常')
    
    return anomalies

# 检测所有广告活动的异常
data['Anomalies'] = data.apply(detect_anomalies, axis=1)

# 保留存在异常的广告活动数据
anomalies_data = data[data['Anomalies'].map(len) > 0]

# 拆分并整理异常现象
anomalies_expanded = anomalies_data.explode('Anomalies')

# 准备最终输出数据
output_data = anomalies_expanded[['campaignId', 'campaignName', 'Anomalies', 'cost_yesterday', 'avg_cost_7d', 'avg_cost_30d']]
output_data.columns = ['异常广告活动ID', '异常广告活动', '异常现象', '昨天的花费', '近7天日均花费', '近30天日均花费']

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\广告活动_预算花费异常_ES_2024-06-10.csv'
output_data.to_csv(output_file_path, index=False)

print("异常检测完成，并已保存到指定文件。")