# filename: process_keywords.py

import pandas as pd

# 读取CSV文件并加载数据
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/预处理.csv"
data = pd.read_csv(file_path)

# 定义执行规则和条件
def apply_keyword_rules(row):
    keyword_bid = row['keywordBid']
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    avg_ACOS_3d = row['ACOS_3d']
    total_sales_7d = row['total_sales14d_7d']
    total_sales_3d = row['total_sales14d_3d']
    total_cost_7d = row['total_cost_7d']
    total_cost_30d = row['total_cost_30d']
    total_cost_3d = row['total_cost_3d']
    click_count_30d = row['total_clicks_30d']
    click_count_7d = row['total_clicks_7d']
    order_1m = row['ORDER_1m']
    new_keyword_bid = keyword_bid

    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
        new_keyword_bid = keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "定义一"
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d > 0.24:
        new_keyword_bid = keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "定义二"
    elif total_cost_7d <= 5 and click_count_7d >= 10 and total_sales_7d == 0 and avg_ACOS_30d <= 0.36:
        new_keyword_bid = keyword_bid - 0.03
        reason = "定义三"
    elif total_cost_7d > 7 and click_count_7d >= 10 and total_sales_7d == 0 and avg_ACOS_30d > 0.5:
        new_keyword_bid = 0.05
        reason = "定义四"
    elif avg_ACOS_7d > 0.5 and avg_ACOS_3d > 0.24 and avg_ACOS_30d > 0.36:
        new_keyword_bid = 0.05
        reason = "定义五"
    elif total_sales_7d == 0 and total_cost_30d >= 10 and click_count_30d >= 15:
        new_keyword_bid = 0.05
        reason = "定义六"
    elif avg_ACOS_7d > 0.24 and avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0 and avg_ACOS_30d <= 0.5 and total_sales_3d == 0:
        new_keyword_bid = keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "定义七"
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and total_sales_3d == 0:
        new_keyword_bid = keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "定义八"
    elif avg_ACOS_7d > 0.5 and total_sales_3d == 0 and avg_ACOS_30d > 0.36:
        new_keyword_bid = 0.05
        reason = "定义九"
    elif avg_ACOS_7d > 0.24 and avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
        new_keyword_bid = keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "定义十"
    elif avg_ACOS_7d > 0.24 and avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0.5 and total_sales_3d == 0:
        new_keyword_bid = keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "定义十一"
    elif avg_ACOS_7d <= 0.24 and total_sales_3d == 0 and total_cost_3d > 3 and total_cost_3d < 5:
        new_keyword_bid = max(keyword_bid - 0.01, 0.05)
        reason = "定义十二"
    elif avg_ACOS_7d <= 0.24 and avg_ACOS_3d > 0.24 and avg_ACOS_3d < 0.36:
        new_keyword_bid = max(keyword_bid - 0.02, 0.05)
        reason = "定义十三"
    elif avg_ACOS_7d <= 0.24 and avg_ACOS_3d > 0.36:
        new_keyword_bid = max(keyword_bid - 0.03, 0.05)
        reason = "定义十四"
    elif total_cost_7d >= 10 and avg_ACOS_30d <= 0.36 and click_count_7d >= 10 and total_sales_7d == 0:
        new_keyword_bid = 0.05
        reason = "定义十五"
    elif total_cost_7d > 5 and total_cost_7d < 10 and avg_ACOS_30d <= 0.36 and click_count_7d >= 10 and total_sales_7d == 0:
        new_keyword_bid = max(keyword_bid - 0.07, 0.05)
        reason = "定义十六"
    else:
        reason = "不符合任何定义"

    return new_keyword_bid, reason

# 应用以上规则
data['new_keywordBid'], data['reason'] = zip(*data.apply(apply_keyword_rules, axis=1))

# 确保竞价大于等于0.05
data['new_keywordBid'] = data['new_keywordBid'].apply(lambda x: max(x, 0.05))

# 输出结果到CSV文件
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/提问策略/手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-12.csv"
columns_to_save = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'new_keywordBid',
                   'targeting', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_7d', 'ACOS_7d', 'ACOS_30d', 'ACOS_3d', 'total_clicks_30d', 'reason']
data[columns_to_save].to_csv(output_file_path, index=False)
print(f"结果已保存到: {output_file_path}")