# filename: 提问策略_自动_优质广告活动.py

import pandas as pd
from datetime import timedelta, datetime

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 假设今天是2024年5月28日，我们需要处理2024年5月27日的数据
yesterday = datetime(2024, 5, 28) - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 滤出昨天的广告活动
data_yesterday = data[data['date'] == yesterday_str]

# 定义条件
condition1 = data_yesterday['avg_ACOS_7d'] < 0.24
condition2 = data_yesterday['ACOS'] < 0.24
condition3 = data_yesterday['cost'] > 0.8 * data_yesterday['Budget']

# 根据所有条件过滤数据
good_campaigns = data_yesterday[condition1 & condition2 & condition3]

# 增加预算
def increase_budget(budget):
    new_budget = budget + budget * 0.2
    return min(new_budget, 50)

good_campaigns['New_Budget'] = good_campaigns['Budget'].apply(increase_budget)

# 保留需要的字段并添加原因字段
good_campaigns['Reason'] = "满足条件: 最近7天的平均ACOS值<0.24, 昨天的ACOS值<0.24, 昨天花费>昨天预算的80%"
columns_to_save = [
    'date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS', 
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'Reason'
]

output_data = good_campaigns[columns_to_save]

# 保存到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_IT_2024-06-06.csv'
output_data.to_csv(output_path, index=False)

# 打印成功信息
print("处理完成，结果已保存在:", output_path)