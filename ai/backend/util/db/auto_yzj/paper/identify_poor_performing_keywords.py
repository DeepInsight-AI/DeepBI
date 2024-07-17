# filename: identify_poor_performing_keywords.py

import pandas as pd

# 读取数据
csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(csv_file_path)

# 定义计算new_keywordBid的函数
def adjust_bid(keywordBid, avg_ACOS_7d):
    new_bid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
    return max(new_bid, 0.05)

# 添加新的列用于结果存放
df['New_keywordBid'] = df['keywordBid']
df['操作原因'] = ""

# 筛选并调整表现较差的关键词
for index, row in df.iterrows():
    keywordBid = row['keywordBid']
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    avg_ACOS_3d = row['ACOS_3d']
    order_1m = row['ORDER_1m']
    sales_7d = row['total_sales14d_7d']
    clicks_7d = row['total_clicks_7d']
    cost_7d = row['total_cost_7d']
    cost_3d = row['total_cost_3d']
    sales_3d = row['total_sales14d_3d']
    clicks_30d = row['total_clicks_30d']
    cost_30d = row['total_cost_30d']
    sales_30d = row['total_sales14d_30d']

    # 定义条件
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d)
        reason = "定义一"
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d >= 0.24:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d)
        reason = "定义二"
    elif clicks_7d >= 10 and sales_7d == 0 and cost_7d <= 5 and avg_ACOS_30d <= 0.36:
        new_bid = max(keywordBid - 0.03, 0.05)
        reason = "定义三"
    elif clicks_7d > 10 and sales_7d == 0 and cost_7d > 7 and avg_ACOS_30d > 0.5:
        new_bid = 0.05
        reason = "定义四"
    elif avg_ACOS_7d > 0.5 and avg_ACOS_3d >= 0.24 and avg_ACOS_30d > 0.36:
        new_bid = 0.05
        reason = "定义五"
    elif sales_30d == 0 and cost_30d >= 10 and clicks_30d >= 15:
        new_bid = 0.05
        reason = "定义六"
    elif 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and order_1m < 5 and sales_3d == 0:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d)
        reason = "定义七"
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and sales_3d == 0:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d)
        reason = "定义八"
    elif avg_ACOS_7d > 0.5 and sales_3d == 0 and avg_ACOS_30d > 0.36:
        new_bid = 0.05
        reason = "定义九"
    elif 0.24 < avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d)
        reason = "定义十"
    elif 0.24 < avg_ACOS_7d <= 0.5 and sales_3d == 0 and avg_ACOS_30d > 0.5:
        new_bid = adjust_bid(keywordBid, avg_ACOS_7d)
        reason = "定义十一"
    elif avg_ACOS_7d <= 0.24 and sales_3d == 0 and 3 < cost_3d < 5:
        new_bid = max(keywordBid - 0.01, 0.05)
        reason = "定义十二"
    elif avg_ACOS_7d <= 0.24 and 0.24 < avg_ACOS_3d < 0.36:
        new_bid = max(keywordBid - 0.02, 0.05)
        reason = "定义十三"
    elif avg_ACOS_7d <= 0.24 and avg_ACOS_3d > 0.36:
        new_bid = max(keywordBid - 0.03, 0.05)
        reason = "定义十四"
    elif clicks_7d >= 10 and sales_7d == 0 and cost_7d >= 10 and avg_ACOS_30d <= 0.36:
        new_bid = 0.05
        reason = "定义十五"
    elif clicks_7d >= 10 and sales_7d == 0 and 5 < cost_7d < 10 and avg_ACOS_30d <= 0.36:
        new_bid = max(keywordBid - 0.07, 0.05)
        reason = "定义十六"
    else:
        continue

    # 更新竞价和操作原因
    df.at[index, 'New_keywordBid'] = new_bid
    df.at[index, '操作原因'] = reason

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_ES_2024-07-15.csv'
df.to_csv(output_file_path, index=False)

print("关键词调整及结果保存已完成。文件保存在:", output_file_path)