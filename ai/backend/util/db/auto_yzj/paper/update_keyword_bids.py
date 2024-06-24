# filename: update_keyword_bids.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义提价的条件和原因标注
conditions = [
    ((df['ACOS_7d'] > 0) & (df['ACOS_7d'] < 0.24) & (df['ACOS_30d'] > 0.5)),
    ((df['ACOS_7d'] > 0) & (df['ACOS_7d'] < 0.24) & (df['ACOS_30d'] > 0.5)),
    ((df['ACOS_7d'] > 0.1) & (df['ACOS_7d'] < 0.24) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24)),
    ((df['ACOS_7d'] > 0) & (df['ACOS_7d'] < 0.1) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24)),
]

choices = [
    '提价0.01, 原因:定义一',
    '提价0.02, 原因:定义二',
    '提价0.03, 原因:定义三',
    '提价0.05, 原因:定义四',
]

# 创建一个新列 '原因'，其中包含符合条件的原因
df['原因'] = None
for condition, choice in zip(conditions, choices):
    df.loc[condition, '原因'] = choice

# 过滤出符合条件的项
filtered_df = df[df['原因'].notnull()]

# 选择需要的列并且输出到新的CSV文件
output_columns = ['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', '原因']
result_df = filtered_df[output_columns]

# 保存输出结果到指定路径
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_ES_2024-06-121.csv'
result_df.to_csv(output_path, index=False)

print("任务完成，结果已保存到指定路径。")