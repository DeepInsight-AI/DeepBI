# filename: sku_filter_and_save.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
df = pd.read_csv(file_path)

# 满足任意一个条件
conditions = [
    # 定义一：sku近7天的总点击数大于10，sku近7天的平均acos值在0.24以上。
    (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24),

    # 定义二：sku近30天的平均acos值大于0.24，sku近七天没有销售额，以及sku近7天总点击数大于10。
    (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10),

    # 定义三：sku近7天的平均acos值在大于0.24小于0.5，sku近30天的平均acos值大于0小于0.24，sku近7天的点击数大于13。
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13),

    # 定义四：sku近7天的平均acos值大于0.24，sku近30天的平均acos值大于0.24。
    (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24),

    # 定义五：sku近7天的平均acos值大于0.5。
    (df['ACOS_7d'] > 0.5),

    # 定义六：sku近30天的总点击数大于13，并且没有销售额。
    (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)
]

# 合并所有条件
combined_conditions = conditions[0]
for condition in conditions[1:]:
    combined_conditions |= condition

# 筛选满足条件的行
filtered_df = df.loc[combined_conditions]

# 为满足条件的行添加“原因”列
filtered_df['reason'] = '满足策略条件'

# 重新选择需要的列
output_columns = [
    'campaignName', 'adGroupName', 'advertisedSku', 
    'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'reason'
]
output_df = filtered_df[output_columns]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_IT_2024-06-05.csv'
output_df.to_csv(output_file_path, index=False)

print(f"结果已保存至 {output_file_path}")