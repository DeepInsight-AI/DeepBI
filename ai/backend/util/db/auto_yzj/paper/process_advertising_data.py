# filename: process_advertising_data.py

import pandas as pd

# 加载数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv"
data = pd.read_csv(file_path)

# 定义 acos 分类
def classify_acos(row):
    if pd.isna(row):
        return 'infinite'
    elif row < 0.2:
        return 'low'
    elif 0.2 <= row < 0.3:
        return 'lower'
    elif 0.3 <= row < 0.5:
        return 'high'
    elif row >= 0.5:
        return 'higher'
    else:
        return None

# 添加 acos 分类列
data['ACOS_7d_class'] = data['ACOS_7d'].apply(classify_acos)

# 定义筛选条件
def condition_1(row):
    return row['ACOS_7d_class'] == 'high' and row['total_clicks_30d'] > 10 and row['total_sales14d_7d'] < 0.1 * row['total_sales14d_30d']

def condition_2(row):
    return row['ACOS_7d_class'] == 'higher' and row['total_sales14d_7d'] < 0.1 * row['total_sales14d_30d']

def condition_3(row):
    return row['total_clicks_30d'] > 10 and row['total_cost_30d'] > 0 and row['total_sales14d_30d'] == 0

# 过滤数据
filtered_data = data[
    data.apply(condition_1, axis=1) | 
    data.apply(condition_2, axis=1) |
    data.apply(condition_3, axis=1)
]

# 添加原因列
def determine_reason(row):
    reasons = []
    if condition_1(row):
        reasons.append("ACOS值较高，点击次数较多，销售额占比极少")
    if condition_2(row):
        reasons.append("ACOS值极高，销售额占比极少")
    if condition_3(row):
        reasons.append("一个月内点击次数>10次，有花费但无销售额")
    return "; ".join(reasons)

filtered_data['reason'] = filtered_data.apply(determine_reason, axis=1)

# 筛选并重命名所需列
filtered_data = filtered_data[[
    'campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'reason'
]]
filtered_data.columns = ['Campaign Name', 'adGroupName', 'cost_7d', 'week_acos', 'sum_clicks', 'searchTerm', 'reason']

# 生成输出文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_劣质搜索词_IT_2024-06-11.csv"
filtered_data.to_csv(output_file_path, index=False)

print(f"Filtered data has been saved to {output_file_path}")