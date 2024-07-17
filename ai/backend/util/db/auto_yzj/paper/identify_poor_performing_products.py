# filename: identify_poor_performing_products.py

import pandas as pd

# 读取CSV数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\预处理.csv"
data = pd.read_csv(file_path)

# 初始化识别出的劣质商品投放
poor_performing_products = []

def check_and_append(row, new_bid, reason):
    poor_performing_products.append({
        "keyword": row['keyword'],
        "keywordId": row['keywordId'],
        "campaignName": row['campaignName'],
        "adGroupName": row['adGroupName'],
        "matchType": row['matchType'],
        "keywordBid": row['keywordBid'],
        "new_keywordBid": new_bid,
        "targeting": row['targeting'],
        "cost": row['total_cost_30d'],
        "clicks": row['total_clicks_30d'],
        "total_cost_7d": row['total_cost_7d'],
        "total_sales14d_7d": row['total_sales14d_7d'],
        "ACOS_7d": row['ACOS_7d'],
        "ACOS_30d": row['ACOS_30d'],
        "total_clicks_30d": row['total_clicks_30d'],
        "reason": reason
    })

# 判断劣质商品投放
for i, row in data.iterrows():
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    clicks_7d = row['total_clicks_7d']
    sales_7d = row['total_sales14d_7d']
    sales_30d = row['total_sales14d_30d']
    cost_30d = row['total_cost_30d']
    bid = row['keywordBid']
    
    if 0.24 < acos_7d <= 0.5 and 0 < acos_30d <= 0.5:
        new_bid = bid / ((acos_7d - 0.24) / 0.24 + 1)
        check_and_append(row, new_bid, "定义一")
    elif acos_7d > 0.5 and acos_30d <= 0.36:
        new_bid = bid / ((acos_7d - 0.24) / 0.24 + 1)
        check_and_append(row, new_bid, "定义二")
    elif clicks_7d >= 10 and sales_7d == 0 and acos_30d <= 0.36:
        new_bid = bid - 0.04
        check_and_append(row, new_bid, "定义三")
    elif clicks_7d > 10 and sales_7d == 0 and acos_30d > 0.5:
        new_bid = "关闭"
        check_and_append(row, new_bid, "定义四")
    elif acos_7d > 0.5 and acos_30d > 0.36:
        new_bid = "关闭"
        check_and_append(row, new_bid, "定义五")
    elif sales_30d == 0 and cost_30d >= 5:
        new_bid = "关闭"
        check_and_append(row, new_bid, "定义六")
    elif sales_30d == 0 and row['total_clicks_30d'] >= 15 and clicks_7d > 0:
        new_bid = "关闭"
        check_and_append(row, new_bid, "定义七")

# 转换为数据框并保存到CSV
output_df = pd.DataFrame(poor_performing_products)
output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放优化\\提问策略\\手动_ASIN_劣质商品投放_v1_1_FR_2024-06-30.csv"
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"劣质商品投放信息处理完毕并保存到 {output_file_path}")