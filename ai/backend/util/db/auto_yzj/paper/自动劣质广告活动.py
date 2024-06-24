# filename: check_ad_performance.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义判断条件

# 定义一
def condition_1(row):
    return (row['ACOS_7d'] > 0.24 and 
            row['ACOS_yesterday'] > 0.24 and 
            row['clicks_yesterday'] >= 10 and 
            row['ACOS_30d'] > row['country_avg_ACOS_1m'])

# 定义二
def condition_2(row):
    return (row['ACOS_7d'] > 0.24 and 
            row['ACOS_yesterday'] > 0.24 and 
            row['cost_yesterday'] >= row['Budget'] * 0.8 and 
            row['ACOS_30d'] > row['country_avg_ACOS_1m'])

# 定义三
def condition_3(row):
    return (row['ACOS_30d'] > 0.24 and 
            row['ACOS_30d'] > row['country_avg_ACOS_1m'] and 
            row['sales_yesterday'] == 0 and 
            row['total_clicks_7d'] >= 15)

# 定义四
def condition_4(row):
    return (row['total_sales14d_30d'] == 0 and 
            row['total_clicks_30d'] >= 75)

# 处理数据
results = []
for idx, row in data.iterrows():
    new_row = row.copy()
    if condition_1(row):
        new_row['New_Budget'] = max(8, row['Budget'] - 5)
        reason = "条件一: 最近7天的平均ACOS值大于0.24, 昨天的ACOS值大于0.24, 昨天的点击数大于等于10, 最近一个月的平均ACOS值大于国家平均"
    elif condition_2(row):
        new_row['New_Budget'] = max(8, row['Budget'] - 5)
        reason = "条件二: 最近7天的平均ACOS值大于0.24, 昨天的ACOS值大于0.24, 昨天的花费超过预算80%, 最近一个月的平均ACOS值大于国家平均"
    elif condition_3(row):
        new_row['New_Budget'] = max(5, row['Budget'] - 5)
        reason = "条件三: 最近一个月的平均ACOS值大于0.24, 最近一个月的平均ACOS值大于国家平均, 最近7天的销售为0, 总点击次数大于等于15"
    elif condition_4(row):
        new_row['New_Budget'] = "关闭"
        reason = "条件四: 最近一个月的总销售为0, 总点击次数大于等于75"

    if new_row['Budget'] != row['Budget']:  # 只有预算变化或要关闭的才保存
        new_row['reason'] = reason
        results.append(new_row)

# 构建输出数据框
output_df = pd.DataFrame(results, columns=[
    'campaignId', 'campaignName', 'Budget', 'New_Budget', 'clicks_yesterday', 
    'ACOS_yesterday', 'ACOS_7d', 'total_clicks_7d', 'sales_yesterday', 
    'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d', 'country_avg_ACOS_1m', 'reason'])

# 输出结果至CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_劣质广告活动_v1_1_IT_2024-06-13.csv'
output_df.to_csv(output_path, index=False)

print(f"Results saved to {output_path}")