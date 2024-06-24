# filename: auto_sku_optimization.py

import pandas as pd

# 文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_ES_2024-06-05.csv'

# 读取 CSV 文件
df = pd.read_csv(file_path)

# 增加辅助列来计算acos平均值
df['ACOS_avg_7d'] = df['ACOS_7d']
df['ACOS_avg_30d'] = df['ACOS_30d']

# 定义筛选条件
condition1 = (df['total_clicks_7d'] > 10) & (df['ACOS_avg_7d'] > 0.24)
condition2 = (df['ACOS_avg_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)
condition3 = (df['ACOS_avg_7d'] > 0.24) & (df['ACOS_avg_7d'] < 0.5) & (df['ACOS_avg_30d'] > 0) & (df['ACOS_avg_30d'] < 0.24) & (df['total_clicks_7d'] > 13)
condition4 = (df['ACOS_avg_7d'] > 0.24) & (df['ACOS_avg_30d'] > 0.24)
condition5 = (df['ACOS_avg_7d'] > 0.5)
condition6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)

# 合并所有条件
conditions = condition1 | condition2 | condition3 | condition4 | condition5 | condition6

# 筛选数据
filtered_df = df[conditions]

# 生成关闭原因
def reason(row):
    if (row['total_clicks_7d'] > 10) and (row['ACOS_avg_7d'] > 0.24):
        return '定义一'
    elif (row['ACOS_avg_30d'] > 0.24) and (row['total_sales14d_7d'] == 0) and (row['total_clicks_7d'] > 10):
        return '定义二'
    elif (row['ACOS_avg_7d'] > 0.24) and (row['ACOS_avg_7d'] < 0.5) and (row['ACOS_avg_30d'] > 0) and (row['ACOS_avg_30d'] < 0.24) and (row['total_clicks_7d'] > 13):
        return '定义三'
    elif (row['ACOS_avg_7d'] > 0.24) and (row['ACOS_avg_30d'] > 0.24):
        return '定义四'
    elif (row['ACOS_avg_7d'] > 0.5):
        return '定义五'
    elif (row['total_clicks_30d'] > 13) and (row['total_sales14d_30d'] == 0):
        return '定义六'
    else:
        return '未知原因'

filtered_df['关闭原因'] = filtered_df.apply(reason, axis=1)

# 选择所需的字段
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭原因']]

# 保存结果到 CSV 文件
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("筛选完毕并保存到文件：", output_path)