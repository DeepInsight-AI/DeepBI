# filename: anomaly_detection_budget.py

import pandas as pd

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
data = pd.read_csv(file_path)

# 计算日均花费
data['avg_cost_7d'] = data['total_cost_7d'] / 7
data['avg_cost_30d'] = data['total_cost_30d'] / 30

# 初始化保存异常的列表
anomalies = []

# 检测异常
for index, row in data.iterrows():
    campaign_id = row['campaignId']
    campaign_name = row['campaignName']
    cost_yesterday = row['cost_yesterday']
    budget = row['campaignBudgetAmount']
    avg_cost_7d = row['avg_cost_7d']
    avg_cost_30d = row['avg_cost_30d']
    
    # 检测超出预算异常
    if cost_yesterday > budget and (avg_cost_7d < budget * 0.5 and avg_cost_30d < budget * 0.5):
        anomalies.append([
            campaign_id, 
            campaign_name, 
            '超出预算', 
            cost_yesterday, 
            avg_cost_7d, 
            avg_cost_30d
        ])
    
    # 检测花费波动异常（如果 avg_cost_7d 和 avg_cost_30d 不为零）
    if avg_cost_7d != 0:
        fluct_7d = abs(cost_yesterday - avg_cost_7d) / avg_cost_7d
        if fluct_7d > 0.3:
            anomalies.append([
                campaign_id, 
                campaign_name, 
                '花费波动 (7天)', 
                cost_yesterday, 
                avg_cost_7d, 
                avg_cost_30d
            ])
            
    if avg_cost_30d != 0:
        fluct_30d = abs(cost_yesterday - avg_cost_30d) / avg_cost_30d
        if fluct_30d > 0.3:
            anomalies.append([
                campaign_id, 
                campaign_name, 
                '花费波动 (30天)', 
                cost_yesterday, 
                avg_cost_7d, 
                avg_cost_30d
            ])

# 保存异常检测结果到新的CSV文件
anomalies_df = pd.DataFrame(anomalies, columns=[
    '广告活动ID', 
    '广告活动名称', 
    '异常现象', 
    '昨天的花费', 
    '近7天日均花费', 
    '近30天日均花费'
])

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\异常检测_广告活动_预算花费异常_v1_0_LAPASA_ES_2024-07-13.csv'
anomalies_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"异常检测完成，结果已保存到 {output_path}")