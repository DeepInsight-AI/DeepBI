# filename: anomaly_detection_ad_campaign.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
data = pd.read_csv(file_path)

# 计算日均花费
data['avg_cost_7d'] = data['total_cost_7d'] / 7
data['avg_cost_30d'] = data['total_cost_30d'] / 30

# 定义异常检测条件
def detect_anomalies(row):
    anomalies = []
    # 检查超出预算异常
    if row['cost_yesterday'] > row['campaignBudgetAmount'] and (row['avg_cost_7d'] < 0.5 * row['campaignBudgetAmount'] or row['avg_cost_30d'] < 0.5 * row['campaignBudgetAmount']):
        anomalies.append('超出预算')
    
    # 检查花费波动异常
    if row['avg_cost_7d'] != 0 and abs(row['cost_yesterday'] - row['avg_cost_7d']) / row['avg_cost_7d'] > 0.3:
        anomalies.append('花费较7天日均花费波动超30%')
    
    if row['avg_cost_30d'] != 0 and abs(row['cost_yesterday'] - row['avg_cost_30d']) / row['avg_cost_30d'] > 0.3:
        anomalies.append('花费较30天日均花费波动超30%')
    
    return ','.join(anomalies)

# 应用检测条件
data['Anomaly Description'] = data.apply(detect_anomalies, axis=1)
anomalies = data[data['Anomaly Description'] != '']

# 选择需要的列输出
output_columns = ['campaignId', 'campaignName', 'Anomaly Description', 'cost_yesterday', 'avg_cost_7d', 'avg_cost_30d']
anomalies = anomalies[output_columns]

# 输出到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\异常检测_广告活动_预算花费异常_v1_0_LAPASA_IT_2024-07-09.csv'
anomalies.to_csv(output_file_path, index=False)

print("异常检测完成，结果已输出至:", output_file_path)