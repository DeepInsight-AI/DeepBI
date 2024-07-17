# filename: analyze_bad_ads.py

import pandas as pd

# 加载 CSV 文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(file_path)

# 定义判别条件和操作逻辑
def classify_ads(row):
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义一"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义二"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] - 0.04
        reason = "定义三"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        new_bid = "关闭"
        reason = "定义四"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        new_bid = "关闭"
        reason = "定义五"
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        new_bid = "关闭"
        reason = "定义六"
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        new_bid = "关闭"
        reason = "定义七"
    else:
        new_bid = row['keywordBid']
        reason = None

    return pd.Series([new_bid, reason])

# 应用判别函数
results = df.apply(classify_ads, axis=1)
df['new_keywordBid'] = results[0]
df['reason'] = results[1]

# 过滤出所有被定义为表现较差的商品投放
bad_ads = df[df['reason'].notna()]

# 选择所需的列
bad_ads = bad_ads[[
    "keyword", "keywordId", "campaignName", "adGroupName", "matchType", "keywordBid", "new_keywordBid",
    "targeting", "total_cost_30d", "total_clicks_30d", "total_sales14d_7d", "total_cost_7d", "ACOS_7d", "ACOS_30d", 
    "total_clicks_7d", "reason"
]]

# 将结果保存到新的 CSV 文件
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_IT_2024-07-04.csv"
bad_ads.to_csv(output_path, index=False)

print("输出已保存到", output_path)