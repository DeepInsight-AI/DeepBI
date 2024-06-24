# filename: process_campaigns.py
import pandas as pd
from datetime import datetime, timedelta

# 设置文件路径
csv_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\优质广告活动_ES_2024-6-02.csv'

# 读取CSV文件
df = pd.read_csv(csv_file)

# 模拟当前时间
current_date = datetime.strptime("2024-05-28", "%Y-%m-%d")
previous_date = current_date - timedelta(days=1)

# 筛选昨天的数据
df['date'] = pd.to_datetime(df['date'])
df_yesterday = df[df['date'] == previous_date]

# 过滤符合条件的广告活动
filtered_campaigns = df_yesterday[
    (df_yesterday['ACOS'] < 0.24) &
    (df_yesterday['avg_ACOS_7d'] < 0.24) &
    (df_yesterday['cost'] > 0.8 * df_yesterday['Budget'])
]

# 增加预算
def update_budget(budget):
    new_budget = min(budget * 1.2, 50)
    return new_budget

filtered_campaigns['Updated_Budget'] = filtered_campaigns['Budget'].apply(update_budget)

# 合并所需信息
final_df = filtered_campaigns[['date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS',
                               'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m']]

# 增加原因列
final_df['原因'] = "广告活动满足条件，增加预算了20%"

# 保存结果至CSV文件
final_df.to_csv(output_file, index=False)

print(f"处理完成，结果已保存至 {output_file}")