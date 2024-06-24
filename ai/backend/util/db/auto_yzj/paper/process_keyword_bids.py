# filename: process_keyword_bids.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 初始化一个空的列表来存储符合条件的记录
results = []

# 遍历所有行，判断是否满足定义条件
for index, row in data.iterrows():
    cause = ''
    if 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24:
        cause = '定义一：降价0.03'
    elif 0.24 < row['ACOS_7d'] < 0.5 and 0.24 < row['ACOS_30d'] < 0.5:
        cause = '定义二：降价0.04'
    elif row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0 and 0.24 < row['ACOS_30d'] < 0.5:
        cause = '定义三：降价0.04'
    elif 0.24 < row['ACOS_7d'] < 0.5 and row['ACOS_30d'] > 0.5:
        cause = '定义四：降价0.05'
    elif row['ACOS_7d'] > 0.5 and 0 < row['ACOS_30d'] < 0.24:
        cause = '定义五：降价0.05'

    # 如果有原因被记录，则添加到结果列表中
    if cause:
        results.append([
            row['campaignName'],
            row['adGroupName'],
            row['keyword'],
            row['ACOS_30d'],
            row['ACOS_7d'],
            cause
        ])

# 将结果转换为DataFrame
result_df = pd.DataFrame(results, columns=[
    'campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', '降价的原因'
])

# 保存结果为新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_ES_2024-06-07.csv'
result_df.to_csv(output_file_path, index=False)

print("文件已经成功保存到:", output_file_path)