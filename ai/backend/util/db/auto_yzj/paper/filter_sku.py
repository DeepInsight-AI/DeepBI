# filename: filter_sku.py

import pandas as pd
from datetime import datetime

# 读取数据集
csv_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
df = pd.read_csv(csv_file)

# 定义今天的日期
today = datetime(2024, 5, 27)

# 筛选符合定义一的 SKU
df['ACOS_7d'] = df['ACOS_7d'].astype(float)  # 确保数据类型正确
df['total_clicks_7d'] = df['total_clicks_7d'].astype(int)

criteria_1 = (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)

# 筛选符合定义二的 SKU
df['ACOS_30d'] = df['ACOS_30d'].astype(float)
df['total_sales14d_7d'] = df['total_sales14d_7d'].astype(float)

criteria_2 = (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)

# 筛选符合定义三的 SKU
criteria_3 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)

# 筛选符合定义四的 SKU
criteria_4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)

# 筛选符合定义五的 SKU
criteria_5 = df['ACOS_7d'] > 0.5

# 筛选符合定义六的 SKU
df['total_clicks_30d'] = df['total_clicks_30d'].astype(int)
criteria_6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)

# 获取满足任意一个条件的 SKU
filtered_df = df[criteria_1 | criteria_2 | criteria_3 | criteria_4 | criteria_5 | criteria_6]

# 增加关闭操作原因列
conditions = [
    (criteria_1),
    (criteria_2),
    (criteria_3),
    (criteria_4),
    (criteria_5),
    (criteria_6)
]
reasons = [
    '定义一：sku近7天的总点击数大于10，sku近7天的平均acos值在0.24以上',
    '定义二：sku近30天的平均acos值大于0.24，sku近七天没有销售额，以及sku近7天总点击数大于10',
    '定义三：sku近7天的平均acos值在大于0.24小于0.5，sku近30天的平均acos值大于0小于0.24，sku近7天的点击数大于13',
    '定义四：sku近7天的平均acos值大于0.24，sku近30天的平均acos值大于0.24',
    '定义五：sku近7天的平均acos值大于0.5',
    '定义六：sku近30天的总点击数大于13，并且没有销售额'
]
filtered_df['关闭操作原因'] = '未定义原因'
for condition, reason in zip(conditions, reasons):
    filtered_df.loc[condition, '关闭操作原因'] = reason

# 提取需要的列并重命名
filtered_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭操作原因']]
filtered_df.columns = ['Campaign Name', 'Ad Group Name', 'Total ACOS 30d', 'Total ACOS 7d', 'Total Clicks 7d', 'SKU', '关闭操作原因']

# 保存结果到新的 CSV 文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_ES_2024-06-121.csv'
filtered_df.to_csv(output_file, index=False)

print(f"筛选结果已保存到 {output_file}")