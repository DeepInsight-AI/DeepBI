# filename: 提问策略_IT_2024-06-05.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\自动定位组优化\\预处理.csv"
df = pd.read_csv(file_path)

# 分类提价原因
df['提价原因'] = ''

# 定义一
mask1 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] < 0.24) & (df['ACOS_30d'] > 0.5)
df.loc[mask1, '提价原因'] = '定义一, 提价0.01'
df.loc[mask1, 'keywordBid'] += 0.01

# 定义二
mask2 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] < 0.24) & (df['ACOS_30d'] > 0.5)
df.loc[mask2, '提价原因'] = '定义二, 提价0.02'
df.loc[mask2, 'keywordBid'] += 0.02

# 定义三
mask3 = (df['ACOS_7d'] > 0.1) & (df['ACOS_7d'] < 0.24) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24)
df.loc[mask3, '提价原因'] = '定义三, 提价0.03'
df.loc[mask3, 'keywordBid'] += 0.03

# 定义四
mask4 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] < 0.1) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24)
df.loc[mask4, '提价原因'] = '定义四, 提价0.05'
df.loc[mask4, 'keywordBid'] += 0.05

# 选择要输出的字段
output_df = df[['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', '提价原因']]

# 生成新的CSV文件
output_file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\自动定位组优化\\提问策略\\自动_优质自动定位组_IT_2024-06-05.csv'
output_df.to_csv(output_file_path, index=False)

print(f"CSV文件已生成: {output_file_path}")