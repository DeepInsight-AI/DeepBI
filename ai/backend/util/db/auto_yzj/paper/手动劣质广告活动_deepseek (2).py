# filename: analyze_and_adjust_budgets_corrected.py
import pandas as pd

# 重新加载数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv')

# 定义函数来检查每个定义的条件
def check_definition_one(row):
    return (row['ACOS_7d'] > 0.24) and (row['ACOS_yesterday'] > 0.24) and (row['clicks_yesterday'] >= 10) and (row['ACOS_30d'] > row['country_avg_ACOS_1m'])

def check_definition_two(row):
    return (row['ACOS_7d'] > 0.24) and (row['ACOS_yesterday'] > 0.24) and (row['cost_yesterday'] > 0.8 * row['Budget']) and (row['ACOS_30d'] > row['country_avg_ACOS_1m'])

def check_definition_three(row):
    return (row['ACOS_30d'] > 0.24) and (row['ACOS_30d'] > row['country_avg_ACOS_1m']) and (row['total_sales14d_7d'] == 0) and (row['total_clicks_7d'] >= 15)

def check_definition_four(row):
    return (row['total_sales14d_30d'] == 0) and (row['total_clicks_30d'] >= 75)

# 应用定义条件到数据集
data['is_definition_one'] = data.apply(check_definition_one, axis=1)
data['is_definition_two'] = data.apply(check_definition_two, axis=1)
data['is_definition_three'] = data.apply(check_definition_three, axis=1)
data['is_definition_four'] = data.apply(check_definition_four, axis=1)

# 计算新的预算
data['New_Budget'] = data['Budget']
data.loc[data['is_definition_one'], 'New_Budget'] = data['Budget'] - 5
data.loc[data['is_definition_two'], 'New_Budget'] = data['Budget'] - 5
data.loc[data['is_definition_three'], 'New_Budget'] = data['Budget'] - 5
data.loc[data['is_definition_four'], 'New_Budget'] = '关闭'

# 确保预算不会低于8或5
data.loc[data['New_Budget'] < 8, 'New_Budget'] = 8
data.loc[data['New_Budget'] < 5, 'New_Budget'] = 5

# 输出结果到CSV文件
output_columns = [
    'campaignId', 'campaignName', 'Budget', 'New_Budget', 'clicks_yesterday',
    'ACOS_yesterday', 'ACOS_7d', 'total_clicks_7d', 'sales_yesterday',
    'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d', 'country_avg_ACOS_1m',
    'reason'
]
data['reason'] = ''
data.loc[data['is_definition_one'], 'reason'] = '定义一'
data.loc[data['is_definition_two'], 'reason'] = '定义二'
data.loc[data['is_definition_three'], 'reason'] = '定义三'
data.loc[data['is_definition_four'], 'reason'] = '定义四'

data[output_columns].to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_v1_1_IT_2024-06-13_deepseek.csv', index=False)

print("结果已保存到CSV文件。")