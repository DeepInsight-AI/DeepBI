# filename: 优质广告活动筛选.py

import pandas as pd

# 加载CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 过滤满足条件的广告活动
def filter_good_campaigns(df):
    # 1. 最近7天的平均ACOS值在0.24以下
    condition_7d_acos = df['avg_ACOS_7d'] < 0.24

    # 2. 昨天的ACOS值在0.24以下
    condition_yesterday_acos = df['ACOS'] < 0.24

    # 3. 昨天花费超过了昨天预算的80%
    condition_budget_spent = df['cost'] > 0.8 * df['Budget']

    # 综合三个条件进行过滤
    filtered_df = df[condition_7d_acos & condition_yesterday_acos & condition_budget_spent]

    # 增加预算并初始化原因字段
    def adjust_budget(row):
        new_budget = min(row['Budget'] * 1.2, 50)
        row['new_Budget'] = new_budget
        row['reason'] = f"表现优异：最近7天的平均ACOS值({row['avg_ACOS_7d']})低于0.24，昨天的ACOS值({row['ACOS']})低于0.24，" \
                         f"昨天花费({row['cost']})超过了昨天预算({row['Budget']})的80%"
        return row

    filtered_df = filtered_df.apply(adjust_budget, axis=1)
    
    return filtered_df

filtered_campaigns = filter_good_campaigns(data)

# 选择所需字段并重新命名以适合输出格式
output_columns = [
    'date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS',
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'reason'
]
filtered_campaigns = filtered_campaigns[output_columns]

# 保存到CSV文件中
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\优质广告活动_FR.csv'
filtered_campaigns.to_csv(output_file_path, index=False)

print(f'结果已保存到 {output_file_path}')