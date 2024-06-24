# filename: process_sku_conditions.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv'
df = pd.read_csv(file_path)

# 创建一个空的数据框来存储结果
filtered_skus_list = []

# 定义一：sku近7天的总点击数大于10，sku近7天的平均ACOS值在0.24以上。
condition1 = (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)
filtered_skus_list.append(df[condition1].assign(reason="定义一")[[
    "campaignName",
    "adGroupName",
    "ACOS_30d",
    "ACOS_7d",
    "total_clicks_7d",
    "advertisedSku",
    "reason"
]])

# 定义二：sku近30天的平均ACOS值大于0.24，sku近7天没有销售额，以及sku近7天总点击数大于10。
condition2 = (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)
filtered_skus_list.append(df[condition2].assign(reason="定义二")[[
    "campaignName",
    "adGroupName",
    "ACOS_30d",
    "ACOS_7d",
    "total_clicks_7d",
    "advertisedSku",
    "reason"
]])

# 定义三：sku近7天的平均ACOS值在大于0.24小于0.5，sku近30天的平均ACOS值大于0小于0.24，sku近7天的点击数大于13。
condition3 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)
filtered_skus_list.append(df[condition3].assign(reason="定义三")[[
    "campaignName",
    "adGroupName",
    "ACOS_30d",
    "ACOS_7d",
    "total_clicks_7d",
    "advertisedSku",
    "reason"
]])

# 定义四：sku近7天的平均ACOS值大于0.24，sku近30天的平均ACOS值大于0.24。
condition4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
filtered_skus_list.append(df[condition4].assign(reason="定义四")[[
    "campaignName",
    "adGroupName",
    "ACOS_30d",
    "ACOS_7d",
    "total_clicks_7d",
    "advertisedSku",
    "reason"
]])

# 定义五：sku近7天的平均ACOS值大于0.5。
condition5 = df['ACOS_7d'] > 0.5
filtered_skus_list.append(df[condition5].assign(reason="定义五")[[
    "campaignName",
    "adGroupName",
    "ACOS_30d",
    "ACOS_7d",
    "total_clicks_7d",
    "advertisedSku",
    "reason"
]])

# 定义六：sku近30天的总点击数大于13，并且没有销售额。
condition6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)
filtered_skus_list.append(df[condition6].assign(reason="定义六")[[
    "campaignName",
    "adGroupName",
    "ACOS_30d",
    "ACOS_7d",
    "total_clicks_7d",
    "advertisedSku",
    "reason"
]])

# 合并所有满足条件的数据
filtered_skus = pd.concat(filtered_skus_list).drop_duplicates()

# 输出到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\手动_关闭SKU_IT_2024-06-05.csv'
filtered_skus.to_csv(output_file_path, index=False)

print("Processed SKUs have been saved to:", output_file_path)