# filename: budget_spending_anomaly_detection.py

import pandas as pd

# 读取CSV文件
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\预处理.csv'
df = pd.read_csv(file_path)

# 计算近7天和近30天的日均花费
df['avg_cost_7d'] = df['total_cost_7d'] / 7
df['avg_cost_30d'] = df['total_cost_30d'] / 30

# 初始化空列表存储异常数据
anomalies = []

# 遍历每一行，判断异常情况
for index, row in df.iterrows():
    anomalies_desc = []

    # 超出预算异常判断
    if row['cost_yesterday'] > row['campaignBudgetAmount'] and \
       (row['avg_cost_7d'] < row['campaignBudgetAmount'] * 0.5 or row['avg_cost_30d'] < row['campaignBudgetAmount'] * 0.5):
        anomalies_desc.append('超出预算')

    # 花费波动异常判断
    if row['avg_cost_7d'] > 0:
        fluctuation_7d = abs(row['cost_yesterday'] - row['avg_cost_7d']) / row['avg_cost_7d']
    else:
        fluctuation_7d = 0

    if row['avg_cost_30d'] > 0:
        fluctuation_30d = abs(row['cost_yesterday'] - row['avg_cost_30d']) / row['avg_cost_30d']
    else:
        fluctuation_30d = 0

    if fluctuation_7d > 0.3 or fluctuation_30d > 0.3:
        anomalies_desc.append('花费波动异常')

    # 如果存在异常，记录相关信息
    if anomalies_desc:
        anomalies.append({
            'campaignId': row['campaignId'],
            'campaignName': row['campaignName'],
            'Anomaly Description': '，'.join(anomalies_desc),
            'cost_yesterday': row['cost_yesterday'],
            'avg_cost_7d': row['avg_cost_7d'],
            'avg_cost_30d': row['avg_cost_30d']
        })

# 转换为DataFrame并输出到CSV文件
anomalies_df = pd.DataFrame(anomalies)
output_file = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\提问策略\\异常检测_广告活动_预算花费异常_v1_0_LAPASA_UK_2024-07-14.csv'
anomalies_df.to_csv(output_file, index=False)

print("异常检测完毕，结果已保存至CSV文件。")