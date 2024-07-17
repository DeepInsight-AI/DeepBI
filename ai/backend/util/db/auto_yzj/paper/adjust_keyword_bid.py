# filename: adjust_keyword_bid.py

import pandas as pd

# 文件路径
csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_FR_2024-07-14.csv'

# 读取CSV文件
df = pd.read_csv(csv_file_path)

# 定义函数来调整竞价
def adjust_bid(current_bid, adjustment, min_bid=0.05):
    new_bid = current_bid / adjustment
    return max(new_bid, min_bid)

# 初始化结果列表
results = []

for index, row in df.iterrows():
    new_keywordBid = row["keywordBid"]
    operation_reason = ""

    avg_ACOS_7d = row["ACOS_7d"]
    avg_ACOS_30d = row["ACOS_30d"]
    avg_ACOS_3d = row["ACOS_3d"]
    clicks_7d = row["total_clicks_7d"]
    sales_7d = row["total_sales14d_7d"]
    cost_7d = row["total_cost_7d"]
    cost_3d = row["total_cost_3d"]
    orders_30d = row["ORDER_1m"]
    clicks_30d = row["total_clicks_30d"]
    sales_30d = row["total_sales14d_30d"]

    if (0.24 < avg_ACOS_7d <= 0.5) and (0 < avg_ACOS_30d <= 0.5) and (orders_30d < 5) and (avg_ACOS_3d >= 0.24):
        adjustment = (avg_ACOS_7d - 0.24) / 0.24 + 1
        new_keywordBid = adjust_bid(row["keywordBid"], adjustment)
        operation_reason = "定义一"
    elif (avg_ACOS_7d > 0.5) and (avg_ACOS_30d <= 0.36) and (avg_ACOS_3d >= 0.24):
        adjustment = (avg_ACOS_7d - 0.24) / 0.24 + 1
        new_keywordBid = adjust_bid(row["keywordBid"], adjustment)
        operation_reason = "定义二"
    elif (clicks_7d >= 10) and (sales_7d == 0) and (cost_7d <= 5) and (avg_ACOS_30d <= 0.36):
        adjustment = 0.03 / 0.24 + 1
        new_keywordBid = adjust_bid(row["keywordBid"], adjustment)
        operation_reason = "定义三"
    elif (clicks_7d > 10) and (sales_7d == 0) and (cost_7d > 7) and (avg_ACOS_30d > 0.5):
        new_keywordBid = max(0.05, new_keywordBid)
        operation_reason = "定义四"
    elif (avg_ACOS_7d > 0.5) and (avg_ACOS_3d >= 0.24) and (avg_ACOS_30d > 0.36):
        new_keywordBid = max(0.05, new_keywordBid)
        operation_reason = "定义五"
    elif (sales_30d == 0) and (row["total_cost_30d"] >= 10) and (clicks_30d >= 15):
        new_keywordBid = max(0.05, new_keywordBid)
        operation_reason = "定义六"
    elif (0.24 < avg_ACOS_7d <= 0.5) and (0 < avg_ACOS_30d <= 0.5) and (orders_30d < 5) and (sales_30d == 0):
        adjustment = (avg_ACOS_7d - 0.24) / 0.24 + 1
        new_keywordBid = adjust_bid(row["keywordBid"], adjustment)
        operation_reason = "定义七"
    elif (avg_ACOS_7d > 0.5) and (avg_ACOS_30d <= 0.36) and (sales_30d == 0):
        adjustment = (avg_ACOS_7d - 0.24) / 0.24 + 1
        new_keywordBid = adjust_bid(row["keywordBid"], adjustment)
        operation_reason = "定义八"
    elif (avg_ACOS_7d > 0.5) and (sales_30d == 0) and (avg_ACOS_30d > 0.36):
        new_keywordBid = max(0.05, new_keywordBid)
        operation_reason = "定义九"
    elif (0.24 < avg_ACOS_7d <= 0.5) and (avg_ACOS_30d > 0.5) and (orders_30d < 5) and (avg_ACOS_3d >= 0.24):
        adjustment = (avg_ACOS_7d - 0.24) / 0.24 + 1
        new_keywordBid = adjust_bid(row["keywordBid"], adjustment)
        operation_reason = "定义十"
    elif (0.24 < avg_ACOS_7d <= 0.5) and (sales_30d == 0) and (avg_ACOS_30d > 0.5):
        adjustment = (avg_ACOS_7d - 0.24) / 0.24 + 1
        new_keywordBid = adjust_bid(row["keywordBid"], adjustment)
        operation_reason = "定义十一"
    elif (avg_ACOS_7d <= 0.24) and (sales_30d == 0) and (3 < cost_3d < 5):
        new_keywordBid = max(0.05, new_keywordBid - 0.01)
        operation_reason = "定义十二"
    elif (avg_ACOS_7d <= 0.24) and (0.24 < avg_ACOS_3d < 0.36):
        new_keywordBid = max(0.05, new_keywordBid - 0.02)
        operation_reason = "定义十三"
    elif (avg_ACOS_7d <= 0.24) and (avg_ACOS_3d > 0.36):
        new_keywordBid = max(0.05, new_keywordBid - 0.03)
        operation_reason = "定义十四"
    elif (clicks_7d >= 10) and (sales_7d == 0) and (cost_7d >= 10) and (avg_ACOS_30d <= 0.36):
        new_keywordBid = max(0.05, new_keywordBid)
        operation_reason = "定义十五"
    elif (clicks_7d >= 10) and (sales_7d == 0) and (5 < cost_7d < 10) and (avg_ACOS_30d <= 0.36):
        new_keywordBid = max(0.05, new_keywordBid - 0.07)
        operation_reason = "定义十六"

    if new_keywordBid != row["keywordBid"]:
        results.append([
            row["keyword"],
            row["keywordId"],
            row["campaignName"],
            row["adGroupName"],
            row["matchType"],
            row["keywordBid"],
            new_keywordBid,
            row["targeting"],
            row["total_cost_30d"],
            row["total_clicks_30d"],
            row["total_cost_7d"],
            row["total_sales14d_30d"],
            row["total_sales14d_7d"],
            row["total_cost_7d"],
            avg_ACOS_7d,
            avg_ACOS_30d,
            avg_ACOS_3d,
            row["total_clicks_30d"],
            operation_reason
        ])

# 列名称
columns = [
    "keyword",
    "keywordId",
    "campaignName",
    "adGroupName",
    "matchType",
    "keywordBid",
    "new_keywordBid",
    "targeting",
    "total_cost_30d",
    "total_clicks_30d",
    "total_cost_7d",
    "total_sales14d_30d",
    "total_sales14d_7d",
    "adGroup_total_cost_7d",
    "avg_ACOS_7d",
    "avg_ACOS_30d",
    "avg_ACOS_3d",
    "total_clicks_30d",
    "operation_reason"
]

# 保存结果至CSV文件
result_df = pd.DataFrame(results, columns=columns)
result_df.to_csv(output_file_path, index=False)
print(f"结果已保存至 {output_file_path}")