# filename: update_bids.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv"
df = pd.read_csv(file_path)

# 增加列以保存调整后的竞价
df['New_keywordBid'] = df['keywordBid']

# 定义函数来调整竞价并返回新竞价、增加金额和原因
def adjust_bid(row):
    reason = ""
    increase = 0.00

    if 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        increase = 0.01
        reason = "定义一"
    elif 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        increase = 0.02
        reason = "定义二"
    elif 0.1 < row['ACOS_7d'] < 0.24 and 0 < row['ACOS_30d'] < 0.24:
        increase = 0.03
        reason = "定义三"
    elif 0 < row['ACOS_7d'] < 0.1 and 0 < row['ACOS_30d'] < 0.24:
        increase = 0.05
        reason = "定义四"

    new_bid = row['keywordBid'] + increase
    return pd.Series([new_bid, increase, reason])

# 应用调整竞价函数到每一行数据
df[['New_keywordBid', 'Increase', 'Reason']] = df.apply(adjust_bid, axis=1)

# 筛选出符合条件的关键词
result_df = df[df['Reason'] != ""]

# 选择要输出的列
output_columns = [
    'campaignName', 'adGroupName', 'keyword', 'keywordBid',
    'New_keywordBid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'Increase', 'Reason'
]
result_df = result_df[output_columns]

# 保存结果到CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_v1_1_IT_2024-06-21.csv"
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("结果已保存到", output_file_path)