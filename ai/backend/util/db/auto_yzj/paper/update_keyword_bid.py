# filename: update_keyword_bid.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 拷贝dataframe用于操作
df_copy = df.copy()

# 增加新列用于存储新的竞价、新的提价金额和提价原因
df_copy['New_keywordBid'] = df_copy['keywordBid']
df_copy['提价'] = 0.0
df_copy['提价原因'] = ""

# 定义1
condition1 = (
    (df_copy['ACOS_7d'] > 0) & (df_copy['ACOS_7d'] <= 0.1) &
    (df_copy['ACOS_30d'] > 0) & (df_copy['ACOS_30d'] <= 0.1) &
    (df_copy['ORDER_1m'] >= 2) &
    (df_copy['ACOS_3d'] > 0) & (df_copy['ACOS_3d'] <= 0.2)
)
df_copy.loc[condition1, 'New_keywordBid'] += 0.05
df_copy.loc[condition1, '提价'] = 0.05
df_copy.loc[condition1, '提价原因'] = "定义1"

# 定义2
condition2 = (
    (df_copy['ACOS_7d'] > 0) & (df_copy['ACOS_7d'] <= 0.1) &
    (df_copy['ACOS_30d'] > 0.1) & (df_copy['ACOS_30d'] <= 0.24) &
    (df_copy['ORDER_1m'] >= 2) &
    (df_copy['ACOS_3d'] > 0) & (df_copy['ACOS_3d'] <= 0.2)
)
df_copy.loc[condition2, 'New_keywordBid'] += 0.03
df_copy.loc[condition2, '提价'] = 0.03
df_copy.loc[condition2, '提价原因'] = "定义2"

# 定义3
condition3 = (
    (df_copy['ACOS_7d'] > 0.1) & (df_copy['ACOS_7d'] <= 0.2) &
    (df_copy['ACOS_30d'] <= 0.1) &
    (df_copy['ORDER_1m'] >= 2) &
    (df_copy['ACOS_3d'] > 0) & (df_copy['ACOS_3d'] <= 0.2)
)
df_copy.loc[condition3, 'New_keywordBid'] += 0.04
df_copy.loc[condition3, '提价'] = 0.04
df_copy.loc[condition3, '提价原因'] = "定义3"

# 定义4
condition4 = (
    (df_copy['ACOS_7d'] > 0.1) & (df_copy['ACOS_7d'] <= 0.2) &
    (df_copy['ACOS_30d'] > 0.1) & (df_copy['ACOS_30d'] <= 0.24) &
    (df_copy['ORDER_1m'] >= 2) &
    (df_copy['ACOS_3d'] > 0) & (df_copy['ACOS_3d'] <= 0.2)
)
df_copy.loc[condition4, 'New_keywordBid'] += 0.02
df_copy.loc[condition4, '提价'] = 0.02
df_copy.loc[condition4, '提价原因'] = "定义4"

# 定义5
condition5 = (
    (df_copy['ACOS_7d'] > 0.2) & (df_copy['ACOS_7d'] <= 0.24) &
    (df_copy['ACOS_30d'] <= 0.1) &
    (df_copy['ORDER_1m'] >= 2) &
    (df_copy['ACOS_3d'] > 0) & (df_copy['ACOS_3d'] <= 0.2)
)
df_copy.loc[condition5, 'New_keywordBid'] += 0.02
df_copy.loc[condition5, '提价'] = 0.02
df_copy.loc[condition5, '提价原因'] = "定义5"

# 定义6
condition6 = (
    (df_copy['ACOS_7d'] > 0.2) & (df_copy['ACOS_7d'] <= 0.24) &
    (df_copy['ACOS_30d'] > 0.1) & (df_copy['ACOS_30d'] <= 0.24) &
    (df_copy['ORDER_1m'] >= 2) &
    (df_copy['ACOS_3d'] > 0) & (df_copy['ACOS_3d'] <= 0.2)
)
df_copy.loc[condition6, 'New_keywordBid'] += 0.01
df_copy.loc[condition6, '提价'] = 0.01
df_copy.loc[condition6, '提价原因'] = "定义6"

# 筛选出需要的列并输出为新的CSV文件
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d',
    'ACOS_7d', 'ACOS_30d', 'ORDER_1m', '提价', '提价原因'
]
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_US_2024-07-12.csv'
df_copy[output_columns].to_csv(output_path, index=False)

print("处理完成，结果已保存到CSV文件中。")