# filename: 提高竞价计算.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv"
df = pd.read_csv(file_path)

# 定义条件函数
def calculate_new_bid(row):
    reason = ""
    increment = 0
    if 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        increment = 0.01
        reason = "Definition One"
    if 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        increment = 0.02
        reason = "Definition Two"
    if 0.1 < row['ACOS_7d'] < 0.24 and 0 < row['ACOS_30d'] < 0.24:
        increment = 0.03
        reason = "Definition Three"
    if 0 < row['ACOS_7d'] < 0.1 and 0 < row['ACOS_30d'] < 0.24:
        increment = 0.05
        reason = "Definition Four"
    return increment, reason

# 应用函数，计算新的竞价和提价原因
df[['Increase', 'Reason']] = df.apply(
    lambda row: pd.Series(calculate_new_bid(row)), axis=1
)

# 计算新的竞价
df['New_keywordBid'] = df['keywordBid'] + df['Increase']

# 筛选出符合条件的行
result_df = df[df['Reason'] != ""]

# 选择必要的列
result_df = result_df[['campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New_keywordBid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'Increase', 'Reason']]

# 保存结果到新的CSV文件
result_output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_v1_11_IT_2024-06-19.csv"
result_df.to_csv(result_output_path, index=False)

print(f"Results have been saved to {result_output_path}")