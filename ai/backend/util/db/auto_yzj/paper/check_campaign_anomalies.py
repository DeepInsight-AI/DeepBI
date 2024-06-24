# filename: check_campaign_anomalies.py

import pandas as pd

# 第一步骤：读取数据
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\预处理.csv'
data = pd.read_csv(file_path)

# 确保数据集包含所有需要的字段
required_fields = ['campaignId', 'campaignName', 'cost_yesterday', 'campaignBudgetAmount', 'total_cost_7d', 'total_cost_30d']
if not all(field in data.columns for field in required_fields):
    raise ValueError("缺少必要的字段")

# 第二步骤：计算日均花费
data['avg_cost_7d'] = data['total_cost_7d'] / 7
data['avg_cost_30d'] = data['total_cost_30d'] / 30

# 第三步骤：异常检查
anomalies = []

for index, row in data.iterrows():
    anomaly_description = []

    # 超出预算异常检查
    if row['cost_yesterday'] > row['campaignBudgetAmount'] and max(row['avg_cost_7d'], row['avg_cost_30d']) < row['campaignBudgetAmount'] * 0.5:
        anomaly_description.append('超出预算异常')

    # 花费波动异常检查
    if (row['avg_cost_7d'] != 0 and abs((row['cost_yesterday'] - row['avg_cost_7d']) / row['avg_cost_7d']) > 0.3) or \
       (row['avg_cost_30d'] != 0 and abs((row['cost_yesterday'] - row['avg_cost_30d']) / row['avg_cost_30d']) > 0.3):
        anomaly_description.append('花费波动异常')

    # 如果存在异常现象，记录异常数据
    if anomaly_description:
        anomalies.append({
            'campaignId': row['campaignId'],
            'campaignName': row['campaignName'],
            'Anomaly Description': '; '.join(anomaly_description),
            'cost_yesterday': row['cost_yesterday'],
            'avg_cost_7d': row['avg_cost_7d'],
            'avg_cost_30d': row['avg_cost_30d']
        })

# 第四步骤：输出结果
anomalies_df = pd.DataFrame(anomalies)
output_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\提问策略\\广告活动_预算花费异常_FR_2024-05-18.csv'
anomalies_df.to_csv(output_path, index=False)

print("异常检查完成，结果已保存至:", output_path)