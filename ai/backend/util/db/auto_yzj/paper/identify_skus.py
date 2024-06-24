# filename: identify_skus.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合条件的数据
today = pd.to_datetime('2024-05-27')
def close_reason(row):
    reasons = []
    if row['total_clicks_7d'] > 10 and row['ACOS_7d'] > 0.24:
        reasons.append("定义一")
    if row['ACOS_30d'] > 0.24 and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 10:
        reasons.append("定义二")
    if 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24 and row['total_clicks_7d'] > 13:
        reasons.append("定义三")
    if row['ACOS_7d'] > 0.24 and row['ACOS_30d'] > 0.24:
        reasons.append("定义四")
    if row['ACOS_7d'] > 0.5:
        reasons.append("定义五")
    if row['total_clicks_30d'] > 13 and row['total_sales14d_30d'] == 0:
        reasons.append("定义六")

    return ', '.join(reasons)

data_filtered = data.copy()
data_filtered['关闭操作的原因'] = data_filtered.apply(close_reason, axis=1)
data_filtered = data_filtered[data_filtered['关闭操作的原因'] != '']

# 从筛选出来的SKU中选择所需的字段
final_data = data_filtered[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭操作的原因']]

# 导出结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_ES_2024-06-10.csv'
final_data.to_csv(output_file_path, index=False)

print(f"筛选和导出的过程已完成，结果保存在: {output_file_path}")