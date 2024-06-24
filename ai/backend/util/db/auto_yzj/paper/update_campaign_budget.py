# filename: update_campaign_budget.py
import pandas as pd
from datetime import datetime, timedelta

# 定义文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_IT_2024-06-05.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 确定今天和昨天的日期 (假设今天是2024年5月28日)
today_date = datetime(2024, 5, 28)
yesterday_date = today_date - timedelta(days=1)

# 将日期列转换为datetime格式
df['date'] = pd.to_datetime(df['date'])

# 筛选表现很好的优质广告活动
filtered_df = df[
    (df['date'] == yesterday_date) &
    (df['avg_ACOS_7d'] < 0.24) &
    (df['ACOS'] < 0.24) &
    (df['cost'] > df['Budget'] * 0.8)
]

# 更新预算
def update_budget(budget):
    new_budget = budget * 1/5
    return min(new_budget, 50)

filtered_df['Updated_Budget'] = filtered_df['Budget'].apply(update_budget)

# 添加原因列
filtered_df['increase_budget_reason'] = '表现很好的优质广告活动. ACOS < 0.24, Cost > 80% of Budget'

# 筛选所需列
output_cols = [
    'date',
    'campaignName',
    'Budget',
    'cost',
    'clicks',
    'ACOS',
    'avg_ACOS_7d',
    'avg_ACOS_1m',
    'clicks_1m',
    'sales_1m',
    'increase_budget_reason'
]

output_df = filtered_df[output_cols]

# 保存到CSV文件
output_df.to_csv(output_file, index=False)

print(f'Result has been saved to {output_file}')