# filename: analyze_ad_campaign.py

import pandas as pd

# 读取CSV文件的路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'

# 确保需要的字段存在
required_columns = [
    'campaignId', 'campaignName', 'cost_yesterday', 'campaignBudgetAmount', 
    'total_cost_7d', 'total_cost_30d'
]

# 读取CSV文件
try:
    df = pd.read_csv(file_path)
except Exception as e:
    print(f"Error reading the CSV file: {e}")
    exit()

# 检查缺失列
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"The following required columns are missing: {', '.join(missing_columns)}")
    exit()

# 提取所需的字段
df = df[required_columns]

# 计算日均花费
df['avg_cost_7d'] = df['total_cost_7d'] / 7
df['avg_cost_30d'] = df['total_cost_30d'] / 30

# 初始化异常描述列表
anomalies = []

# 判断异常条件
for index, row in df.iterrows():
    campaign_id = row['campaignId']
    campaign_name = row['campaignName']
    cost_yesterday = row['cost_yesterday']
    budget_amount = row['campaignBudgetAmount']
    avg_cost_7d = row['avg_cost_7d']
    avg_cost_30d = row['avg_cost_30d']
    
    # 检查异常
    anomaly_description = ""
    
    # 超出预算异常
    if (cost_yesterday > budget_amount) and (avg_cost_7d < budget_amount * 0.5) and (avg_cost_30d < budget_amount * 0.5):
        anomaly_description += "超出预算异常; "
    
    # 波动异常，确保平均花费不是零
    if (avg_cost_7d > 0 and abs(cost_yesterday - avg_cost_7d) / avg_cost_7d > 0.3) or (avg_cost_30d > 0 and abs(cost_yesterday - avg_cost_30d) / avg_cost_30d > 0.3):
        anomaly_description += "波动异常; "
    
    # 如果存在异常，加入到结果列表
    if anomaly_description:
        anomalies.append({
            'campaignId': campaign_id,
            'campaignName': campaign_name,
            'Anomaly Description': anomaly_description.strip(),
            'cost_yesterday': cost_yesterday,
            'avg_cost_7d': avg_cost_7d,
            'avg_cost_30d': avg_cost_30d
        })

# 转换成DataFrame并保存到CSV文件
anomalies_df = pd.DataFrame(anomalies)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\广告活动_预算花费异常_IT_2024-06-08.csv'
anomalies_df.to_csv(output_file_path, index=False)

# 打印输出文件路径
print(f"The anomalies have been saved to {output_file_path}")