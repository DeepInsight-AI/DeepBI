# filename: budget_optimization_relaxed.py

import pandas as pd
from datetime import datetime, timedelta

# 定义文件路径
input_file = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\预算优化\\预处理.csv"
output_file = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\预算优化\\提问策略\\自动_劣质广告活动_ES_2024-06-07.csv"

# 读取CSV文件
data = pd.read_csv(input_file)

# 获取昨天的日期
today = datetime(2024, 5, 28)
yesterday = today - timedelta(1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 放宽条件进行过滤
filtered_data = data[
    (data['date'] == yesterday_str) &
    (data['avg_ACOS_7d'] > 0.2) &  # 将阈值降低到0.2
    (
        ((data['ACOS'] > 0.2) & (data['clicks'] >= 5) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])) |  # 将ACOS和点击数阈值降低
        ((data['ACOS'] > 0.2) & (data['cost'] > 0.7 * data['Budget']) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])) |  # 将花费比例降低
        ((data['avg_ACOS_1m'] > 0.2) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m']) & (data['clicks_7d'] >= 10))  # 将7天点击数阈值降低
    )
]

print("Relaxed Condition - Filtered Data:")
print(filtered_data)

if not filtered_data.empty:
    # 更新预算
    def update_budget(row):
        try:
            if row['avg_ACOS_1m'] > 0.2 and row['avg_ACOS_7d'] > 0.2 and row['ACOS'] > 0.2 and row['clicks'] >= 5 and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']:
                new_budget = max(8, row['Budget'] - 5)
                reason = "定义一（放宽条件）"
            elif row['avg_ACOS_1m'] > 0.2 and row['avg_ACOS_7d'] > 0.2 and row['ACOS'] > 0.2 and row['cost'] > 0.7 * row['Budget'] and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']:
                new_budget = max(8, row['Budget'] - 5)
                reason = "定义二（放宽条件）"
            elif row['avg_ACOS_1m'] > 0.2 and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m'] and row['clicks_7d'] >= 10:
                new_budget = max(5, row['Budget'] - 5)
                reason = "定义三（放宽条件）"
            else:
                new_budget = row['Budget']
                reason = "未满足任何定义"
        except Exception as e:
            new_budget = row['Budget']
            reason = f"错误: {e}"
        print(f"Processing row: {row.to_dict()}")
        print(f"New budget: {new_budget}, Reason: {reason}")
        return new_budget, reason

    # 使用apply方法更新预算和理由列
    filtered_data['new_budget'], filtered_data['reason'] = zip(*filtered_data.apply(update_budget, axis=1))

    # 选择需要的列，并保存到新的CSV文件
    result = filtered_data[['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'clicks_7d', 
                            'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_budget', 'reason']]

    result.to_csv(output_file, index=False)

    print("新的预算调整已保存到 CSV 文件:", output_file)
else:
    print("No records found matching the relaxed criteria.")