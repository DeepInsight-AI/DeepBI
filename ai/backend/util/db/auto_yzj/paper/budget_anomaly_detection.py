# filename: budget_anomaly_detection.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
df = pd.read_csv(file_path)

# 计算日均花费
df['daily_cost_7d'] = df['total_cost_7d'] / 7
df['daily_cost_30d'] = df['total_cost_30d'] / 30

# 存储异常的广告活动
anomalies = []

# 检查每个广告活动
for index, row in df.iterrows():
    campaign_id = row['campaignId']
    campaign_name = row['campaignName']
    cost_yesterday = row['cost_yesterday']
    budget = row['campaignBudgetAmount']
    daily_cost_7d = row['daily_cost_7d']
    daily_cost_30d = row['daily_cost_30d']
    
    # 超出预算检查
    if cost_yesterday > budget and (daily_cost_7d < budget * 0.5 and daily_cost_30d < budget * 0.5):
        anomalies.append([campaign_id, campaign_name, '超出预算异常', cost_yesterday, daily_cost_7d, daily_cost_30d])
    
    # 波动异常检查
    if daily_cost_7d != 0 and abs(cost_yesterday - daily_cost_7d) / daily_cost_7d > 0.3:
        anomalies.append([campaign_id, campaign_name, '波动异常', cost_yesterday, daily_cost_7d, daily_cost_30d])
    elif daily_cost_30d != 0 and abs(cost_yesterday - daily_cost_30d) / daily_cost_30d > 0.3:
        anomalies.append([campaign_id, campaign_name, '波动异常', cost_yesterday, daily_cost_7d, daily_cost_30d])

# 转换为DataFrame并导出CSV
anomalies_df = pd.DataFrame(anomalies, columns=['campaignId', 'Campaign Name', 'Anomaly Description', 'cost_yesterday', 'daily_cost_7d', 'daily_cost_30d'])
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\异常检测_广告活动_预算花费异常_v1_0_LAPASA_IT_2024-07-15.csv'
anomalies_df.to_csv(output_file_path, index=False)

print("异常检测完成，结果已保存至:", output_file_path)