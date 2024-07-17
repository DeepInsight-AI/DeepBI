# filename: detect_budget_anomalies.py
import pandas as pd

# 读取CSV文件
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\预处理.csv'
data = pd.read_csv(file_path)

# 计算日均花费
data['daily_cost_7d'] = data['total_cost_7d'] / 7
data['daily_cost_30d'] = data['total_cost_30d'] / 30

# 定义异常列表
anomalies = []

# 检查每个广告活动的异常情况
for _, row in data.iterrows():
    campaign_id = row['campaignId']
    campaign_name = row['campaignName']
    cost_yesterday = row['cost_yesterday']
    budget = row['campaignBudgetAmount']
    daily_cost_7d = row['daily_cost_7d']
    daily_cost_30d = row['daily_cost_30d']
    
    # 超出预算异常检查
    if cost_yesterday > budget and (daily_cost_7d < budget * 1.5 and daily_cost_30d < budget * 1.5):
        anomalies.append([
            campaign_id, campaign_name, '超出预算异常', cost_yesterday, daily_cost_7d, daily_cost_30d
        ])
    
    # 波动异常检查
    if daily_cost_7d > 0:
        fluctuation_7d = abs(cost_yesterday - daily_cost_7d) / daily_cost_7d
        if fluctuation_7d > 0.3:
            anomalies.append([
                campaign_id, campaign_name, '花费波动异常', cost_yesterday, daily_cost_7d, daily_cost_30d
            ])
    if daily_cost_30d > 0:
        fluctuation_30d = abs(cost_yesterday - daily_cost_30d) / daily_cost_30d
        if fluctuation_30d > 0.3:
            anomalies.append([
                campaign_id, campaign_name, '花费波动异常', cost_yesterday, daily_cost_7d, daily_cost_30d
            ])

# 将异常结果转换为DataFrame
anomalies_df = pd.DataFrame(anomalies, columns=[
    'campaignId', 'Campaign Name', 'Anomaly Description', 'Yesterday Cost', '7-day Avg Cost', '30-day Avg Cost'
])

# 保存到CSV文件
output_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\提问策略\\异常检测_广告活动_预算花费异常_v1_0_LAPASA_US_2024-07-14.csv'
anomalies_df.to_csv(output_path, index=False)

print(f"预算花费异常检测完成，结果保存在 {output_path}")