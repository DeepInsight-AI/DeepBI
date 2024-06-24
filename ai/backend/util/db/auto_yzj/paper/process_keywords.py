# filename: process_keywords.py
import pandas as pd

# 文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_1_ES_2024-06-20.csv'

# 读取数据
df = pd.read_csv(file_path)

# 初始化新的bids和原因列
df['New Bid'] = df['keywordBid']
df['Reason'] = ''

# 定义条件及操作
conditions = [
    # 定义一
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24),
    # 定义二
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.5),
    # 定义三
    (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0) & (df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.5),
    # 定义四
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0.5),
    # 定义五
    (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24),
    # 定义六
    (df['ORDER_1m'] == 0) & (df['total_clicks_30d'] > 13) & (df['total_clicks_7d'] > 0),
    # 定义七
    (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0) & (df['ACOS_30d'] > 0.5),
    # 定义八
    (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] > 0.24)
]

values = [
    (0.03, 'Lower bid by 0.03 due to condition 1'),
    (0.04, 'Lower bid by 0.04 due to condition 2'),
    (0.04, 'Lower bid by 0.04 due to condition 3'),
    (0.05, 'Lower bid by 0.05 due to condition 4'),
    (0.05, 'Lower bid by 0.05 due to condition 5'),
    ('关闭', 'Close due to condition 6'),
    ('关闭', 'Close due to condition 7'),
    ('关闭', 'Close due to condition 8')
]

# 应用条件
for condition, (bid_change, reason) in zip(conditions, values):
    if isinstance(bid_change, str) and bid_change == "关闭":
        df.loc[condition, 'New Bid'] = bid_change
    else:
        df.loc[condition, 'New Bid'] = df['keywordBid'] - bid_change

    df.loc[condition, 'Reason'] = reason

# 选择所需的列
output_df = df[['campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New Bid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'Reason']]

# 保存结果
output_df.to_csv(output_path, index=False)

print("Processing complete. Output saved to:", output_path)