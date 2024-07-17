# filename: detect_poor_performance.py

import pandas as pd

# 读取CSV文件
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\预处理.csv'
df = pd.read_csv(file_path)

# 定义新的列
df['new_keywordBid'] = df['keywordBid']  # 初始化为当前竞价
df['action'] = ''  # 初始化为空
df['reason'] = ''  # 初始化为空

# 条件筛选并修改竞价或标记关闭
for idx, row in df.iterrows():
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        df.at[idx, 'new_keywordBid'] = row['keywordBid'] / (((row['ACOS_7d'] - 0.24) / 0.24) + 1)
        df.at[idx, 'reason'] = '近期和一个月平均ACOS值均较高，调整竞价'
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        df.at[idx, 'new_keywordBid'] = row['keywordBid'] / (((row['ACOS_7d'] - 0.24) / 0.24) + 1)
        df.at[idx, 'reason'] = '近七天平均ACOS值偏高，调整竞价'
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        df.at[idx, 'new_keywordBid'] = max(0, row['keywordBid'] - 0.04)
        df.at[idx, 'reason'] = '近期点击多但没有销售，降低竞价'
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        df.at[idx, 'new_keywordBid'] = '关闭'
        df.at[idx, 'reason'] = '近期点击多但没有销售并且ACOS>0.5，关闭该词'
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        df.at[idx, 'new_keywordBid'] = '关闭'
        df.at[idx, 'reason'] = '近七天和一个月ACOS值较高，关闭该词'
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        df.at[idx, 'new_keywordBid'] = '关闭'
        df.at[idx, 'reason'] = '最近一个月销售=0且花费>5，关闭该词'
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        df.at[idx, 'new_keywordBid'] = '关闭'
        df.at[idx, 'reason'] = '最近一个月销售=0点击数>15且近七天有点击，关闭该词'

# 筛选符合条件的商品投放
result_df = df[df['reason'] != '']

# 输出到CSV文件
output_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\提问策略\\手动_ASIN_劣质商品投放_v1_1_ES_2024-06-30.csv'
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(result_df.head())  # 打印输出前几行验证结果