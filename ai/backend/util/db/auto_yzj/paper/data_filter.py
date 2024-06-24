# filename: data_filter.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\SKU优化\\预处理.csv"
df = pd.read_csv(file_path)

# 定义1: sku近7天的总点击数大于10，sku近7天的平均acos值在0.24以上。
df['ACOS_7d'] = df['ACOS_7d'].astype(float)
condition_1 = (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)

# 定义2: sku近30天的平均acos值大于0.24，sku近七天没有销售额，以及sku近7天总点击数大于10。
df['ACOS_30d'] = df['ACOS_30d'].astype(float)
condition_2 = (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)

# 定义3: sku近7天的平均acos值大于0.24小于0.5，sku近30天的平均acos值大于0小于0.24，sku近7天的点击数大于13。
condition_3 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)

# 定义4: sku近7天的平均acos值大于0.24，sku近30天的平均acos值大于0.24。
condition_4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)

# 定义5: sku近7天的平均acos值大于0.5。
condition_5 = df['ACOS_7d'] > 0.5

# 定义6: sku近30天的总点击数大于13，并且没有销售额。
condition_6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)

# 任意一个条件为True
final_condition = condition_1 | condition_2 | condition_3 | condition_4 | condition_5 | condition_6

# 过滤满足条件的行
filtered_df = df[final_condition]

# 添加"关闭原因"列，并根据满足的定义添加原因
filtered_df['关闭原因'] = None
filtered_df.loc[condition_1, '关闭原因'] = '定义1'
filtered_df.loc[condition_2, '关闭原因'] = '定义2'
filtered_df.loc[condition_3, '关闭原因'] = '定义3'
filtered_df.loc[condition_4, '关闭原因'] = '定义4'
filtered_df.loc[condition_5, '关闭原因'] = '定义5'
filtered_df.loc[condition_6, '关闭原因'] = '定义6'

# 提取所需列
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭原因']]

# 保存结果到新的CSV文件
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\SKU优化\\提问策略\\手动_关闭SKU_IT_2024-06-06.csv"
result_df.to_csv(output_path, index=False)

print("操作成功，文件已保存至:", output_path)