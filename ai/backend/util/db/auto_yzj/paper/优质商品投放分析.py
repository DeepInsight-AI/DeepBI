# filename: 优质商品投放分析.py
import pandas as pd

# 读取数据
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(data_path)

# 定义过滤条件和提价策略
def update_bid(row):
    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        return 0.05, '定义一'
    elif 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        return 0.03, '定义二'
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        return 0.04, '定义三'
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        return 0.02, '定义四'
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        return 0.02, '定义五'
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        return 0.01, '定义六'
    else:
        return 0, None

# 根据定义过滤数据并增加提价列
df['提价'], df['提价原因'] = zip(*df.apply(update_bid, axis=1))

# 新增竞价列
df['New_keywordBid'] = df.apply(lambda row: row['keywordBid'] + row['提价'] if row['提价'] > 0 else row['keywordBid'], axis=1)

# 过滤出需要提价的数据
result_df = df[df['提价'] > 0]

# 输出列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid',
    'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d',
    'ACOS_30d', 'ORDER_1m', '提价', '提价原因'
]

# 输出结果到CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_UK_2024-07-10.csv'
result_df.to_csv(output_path, columns=output_columns, index=False)

print(f"结果已保存到: {output_path}")