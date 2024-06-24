# filename: adjust_budget.py
import pandas as pd
from datetime import datetime, timedelta

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 假定今天是2024年5月28日
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)

# 打印原始数据的前几行以便调试
print("原始数据预览:\n", data.head())

# 筛选符合定义一、定义二、定义三的劣质广告活动
conditions = (
    (data['avg_ACOS_7d'] > 0.24) & (data['ACOS'] > 0.24) & (data['clicks'] >= 10) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m']) |
    (data['avg_ACOS_7d'] > 0.24) & (data['ACOS'] > 0.24) & (data['cost'] > 0.8 * data['Budget']) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m']) |
    (data['avg_ACOS_1m'] > 0.24) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m']) & (data['clicks_7d'] >= 15) & (data['sales_1m'] == 0)
)

filtered_data = data[conditions].copy()

# 打印符合条件的部分数据预览
print("符合条件的数据预览:\n", filtered_data.head())
print("符合条件的数据行数:\n", filtered_data.shape[0])

# 调整预算函数
def adjust_budget(row):
    new_budget = row['Budget']
    reason = ''
    if row['avg_ACOS_7d'] > 0.24 and row['ACOS'] > 0.24 and row['clicks'] >= 10 and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']:
        reason = '定义一'
        new_budget = max(new_budget - 5, 8)
    elif row['avg_ACOS_7d'] > 0.24 and row['ACOS'] > 0.24 and row['cost'] > 0.8 * row['Budget'] and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']:
        reason = '定义二'
        new_budget = max(new_budget - 5, 8)
    elif row['avg_ACOS_1m'] > 0.24 and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m'] and row['clicks_7d'] >= 15 and row['sales_1m'] == 0:
        reason = '定义三'
        new_budget = max(new_budget - 5, 5)
    
    return pd.Series([new_budget, reason], index=['new_Budget', 'Reason'])

# 使用apply函数调整每个行的预算
adjusted_budget = filtered_data.apply(adjust_budget, axis=1)

# 打印调整后的预算预览及长度
print("调整后的预算预览:\n", adjusted_budget.head())
print("调整后的预算行数:\n", adjusted_budget.shape[0])
print("符合条件的数据行数（再次验证）：\n", filtered_data.shape[0])

# 验证调整后的预算数据长度是否与原始数据长度匹配
if adjusted_budget.shape[0] != filtered_data.shape[0]:
    print("错误：调整后的预算数据长度与过滤后的数据长度不匹配。")
    print("调整后的预算数据：\n", adjusted_budget)
    print("过滤后的数据：\n", filtered_data)
    print(f"调整后的预算数据行数: {adjusted_budget.shape[0]}, 过滤后的数据行数: {filtered_data.shape[0]}")
else:
    # 如果长度匹配，将调整后的预算合并到filtered_data
    filtered_data[['new_Budget', 'Reason']] = adjusted_budget

    # 确保输出包含所需的列（添加条件判断，确保列存在于数据集中）
    output_columns = [
        col for col in [
            'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'clicks_7d',
            'sales_1m', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_Budget', 'Reason'
        ] if col in filtered_data.columns
    ]

    output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_劣质广告活动_ES_2024-06-07.csv'
    filtered_data[output_columns].to_csv(output_path, index=False)

    print(f'Filtered data has been saved to {output_path}')