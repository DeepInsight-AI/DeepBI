# filename: optimize_product_bids.py

import pandas as pd

# 读取CSV文件
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/预处理.csv'
df = pd.read_csv(file_path)

# 条件判断和竞价调整
def adjust_bids(df):

    # 定义一
    cond1 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.1) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.1) & (df['ORDER_1m'] >= 2)
    df.loc[cond1, 'New_keywordBid'] = df['keywordBid'] + 0.05
    df.loc[cond1, 'adjustment_reason'] = '定义一'

    # 定义二
    cond2 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.1) & (df['ACOS_30d'] > 0.1) & (df['ACOS_30d'] <= 0.24) & (df['ORDER_1m'] >= 2)
    df.loc[cond2, 'New_keywordBid'] = df['keywordBid'] + 0.03
    df.loc[cond2, 'adjustment_reason'] = '定义二'

    # 定义三
    cond3 = (df['ACOS_7d'] > 0.1) & (df['ACOS_7d'] <= 0.2) & (df['ACOS_30d'] <= 0.1) & (df['ORDER_1m'] >= 2)
    df.loc[cond3, 'New_keywordBid'] = df['keywordBid'] + 0.04
    df.loc[cond3, 'adjustment_reason'] = '定义三'

    # 定义四
    cond4 = (df['ACOS_7d'] > 0.1) & (df['ACOS_7d'] <= 0.2) & (df['ACOS_30d'] > 0.1) & (df['ACOS_30d'] <= 0.24) & (df['ORDER_1m'] >= 2)
    df.loc[cond4, 'New_keywordBid'] = df['keywordBid'] + 0.02
    df.loc[cond4, 'adjustment_reason'] = '定义四'

    # 定义五
    cond5 = (df['ACOS_7d'] > 0.2) & (df['ACOS_7d'] <= 0.24) & (df['ACOS_30d'] <= 0.1) & (df['ORDER_1m'] >= 2)
    df.loc[cond5, 'New_keywordBid'] = df['keywordBid'] + 0.02
    df.loc[cond5, 'adjustment_reason'] = '定义五'

    # 定义六
    cond6 = (df['ACOS_7d'] > 0.2) & (df['ACOS_7d'] <= 0.24) & (df['ACOS_30d'] > 0.1) & (df['ACOS_30d'] <= 0.24) & (df['ORDER_1m'] >= 2)
    df.loc[cond6, 'New_keywordBid'] = df['keywordBid'] + 0.01
    df.loc[cond6, 'adjustment_reason'] = '定义六'

    return df

# 应用竞价调整函数
df = adjust_bids(df)

# 选择输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 
    'targeting', 'total_cost_30d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'adjustment_reason'
]

# 过滤符合条件的行
df_filtered = df[df['adjustment_reason'].notnull()]

# 输出结果到新的CSV文件
output_file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/提问策略/手动_ASIN_优质商品投放_v1_1_LAPASA_ES_2024-07-02.csv'
df_filtered.to_csv(output_file_path, index=False)
print(f"输出结果到 {output_file_path}")
