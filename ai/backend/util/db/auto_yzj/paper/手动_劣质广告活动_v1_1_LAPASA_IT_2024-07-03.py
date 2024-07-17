# filename: 手动_劣质广告活动_v1_1_LAPASA_IT_2024-07-03.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\预算优化\预处理.csv"
data = pd.read_csv(file_path)

# 定义一
def cond1(row):
    return (
        row['ACOS_7d'] > 0.35 and 
        row['ACOS_yesterday'] > 0.35 and 
        row['clicks_yesterday'] >= 10 and 
        row['Budget'] > 8
    )

# 定义二
def cond2(row):
    return (
        row['ACOS_30d'] > 0.35 and 
        row['total_sales14d_7d'] == 0 and 
        row['total_clicks_7d'] >= 15 and 
        row['Budget'] > 5
    )

# 定义三
def cond3(row):
    return (
        row['total_sales14d_30d'] == 0 and 
        row['total_clicks_30d'] >= 75
    )

# 新的预算计算
def calculate_new_budget(row):
    if cond1(row):
        while row['Budget'] > 8:
            row['Budget'] -= 5
        row['Reason'] = "定义一"
    elif cond2(row):
        while row['Budget'] > 5:
            row['Budget'] -= 5
        row['Reason'] = "定义二"
    elif cond3(row):
        row['Budget'] = "关闭"
        row['Reason'] = "定义三"
    else:
        row['Reason'] = None
    row['New Budget'] = row['Budget']
    return row

# 筛选表现较差的劣质广告活动并计算新的预算
filtered_data = data.apply(calculate_new_budget, axis=1)

# 添加需要输出的列名
output_columns = [
    'campaignId', 'campaignName', 'Budget', 'New Budget', 'Reason', 'clicks_yesterday', 
    'ACOS_yesterday', 'ACOS_7d', 'total_clicks_7d', 'total_sales14d_7d', 
    'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d', 'country_avg_ACOS_1m'
]

# 筛选需要输出的列
output_data = filtered_data[output_columns]

# 输出结果到指定CSV文件
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_v1_1_LAPASA_IT_2024-07-03.csv"
output_data.to_csv(output_path, index=False)

print(f"识别并操作后的劣质广告活动数据已保存到 {output_path}")