# filename: filter_and_output_corrected_v3.py
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv')

# 检查数据集中是否包含 'sales_7d' 字段
if 'sales_7d' not in data.columns:
    print("'sales_7d' 字段不存在于数据集中，将移除相关筛选条件。")

# 筛选满足定义一的广告活动
def filter_definition_one(data):
    filtered_data = data[(data['avg_ACOS_7d'] > 0.24) &
                         (data['ACOS'] > 0.24) &
                         (data['clicks'] >= 10) &
                         (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])]
    return filtered_data

# 筛选满足定义二的广告活动
def filter_definition_two(data):
    filtered_data = data[(data['avg_ACOS_7d'] > 0.24) &
                         (data['ACOS'] > 0.24) &
                         (data['cost'] > 0.8 * data['Budget']) &
                         (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])]
    return filtered_data

# 筛选满足定义三的广告活动，移除 'sales_7d' 条件
def filter_definition_three(data):
    filtered_data = data[(data['avg_ACOS_1m'] > 0.24) &
                         (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m']) &
                         (data['clicks_7d'] >= 15)]
    return filtered_data

# 应用筛选条件
filtered_data_one = filter_definition_one(data)
filtered_data_two = filter_definition_two(data)
filtered_data_three = filter_definition_three(data)

# 合并筛选结果
filtered_data = pd.concat([filtered_data_one, filtered_data_two, filtered_data_three])

# 计算新的预算
filtered_data['new_Budget'] = filtered_data.apply(lambda row: max(8, row['Budget'] - 5), axis=1)

# 输出结果到CSV文件
output_columns = ['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'clicks_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_Budget']
output_data = filtered_data[output_columns]
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\劣质广告活动_FR_2024-5-28_deepseek.csv', index=False)

print("数据已成功输出到CSV文件。")