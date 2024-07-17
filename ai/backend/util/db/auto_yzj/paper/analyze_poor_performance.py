# filename: analyze_poor_performance.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(file_path)

# 定义要追加的列
df['New_keywordBid'] = df['keywordBid']
df['action_reason'] = ""

# 定义判定逻辑
for index, row in df.iterrows():
    ACOS_7d = row['ACOS_7d']
    ACOS_30d = row['ACOS_30d']
    keywordBid = row['keywordBid']
    total_clicks_7d = row['total_clicks_7d']
    total_sales14d_7d = row['total_sales14d_7d']
    total_cost_7d = row['total_cost_7d']
    total_sales14d_30d = row['total_sales14d_30d']
    total_clicks_30d = row['total_clicks_30d']
    total_cost_30d = row['total_cost_30d']

    # 定义一
    if 0.24 < ACOS_7d <= 0.5 and 0 < ACOS_30d <= 0.5:
        new_bid = keywordBid / ((ACOS_7d - 0.24) / 0.24 + 1)
        df.at[index, 'New_keywordBid'] = new_bid
        df.at[index, 'action_reason'] = "定义一"

    # 定义二
    if ACOS_7d > 0.5 and ACOS_30d <= 0.36:
        new_bid = keywordBid / ((ACOS_7d - 0.24) / 0.24 + 1)
        df.at[index, 'New_keywordBid'] = new_bid
        df.at[index, 'action_reason'] = "定义二"

    # 定义三
    if total_clicks_7d >= 10 and total_sales14d_7d == 0 and ACOS_30d <= 0.36:
        new_bid = max(keywordBid - 0.04, 0)  # 确保竞价不能低于0
        df.at[index, 'New_keywordBid'] = new_bid
        df.at[index, 'action_reason'] = "定义三"

    # 定义四
    if total_clicks_7d > 10 and total_sales14d_7d == 0 and ACOS_30d > 0.5:
        df.at[index, 'New_keywordBid'] = "关闭"
        df.at[index, 'action_reason'] = "定义四"

    # 定义五
    if ACOS_7d > 0.5 and ACOS_30d > 0.36:
        df.at[index, 'New_keywordBid'] = "关闭"
        df.at[index, 'action_reason'] = "定义五"

    # 定义六
    if total_sales14d_30d == 0 and total_cost_30d >= 5:
        df.at[index, 'New_keywordBid'] = "关闭"
        df.at[index, 'action_reason'] = "定义六"

    # 定义七
    if total_sales14d_30d == 0 and total_clicks_30d >= 15 and total_clicks_7d > 0:
        df.at[index, 'New_keywordBid'] = "关闭"
        df.at[index, 'action_reason'] = "定义七"

# 筛选出需要操作的商品投放
result_df = df[df['action_reason'] != ""]

# 保存结果到CSV文件
output_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_DELOMO_ES_2024-07-09.csv"
result_df.to_csv(output_file, index=False)

print(f"结果已保存到 {output_file}")