# filename: analyze_product_ads.py

import pandas as pd

# 读取CSV文件
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/预处理.csv'
data = pd.read_csv(file_path)

# 根据定义的条件处理数据
def analyze_ads(data):
    results = []

    for _, row in data.iterrows():
        keywordId = row['keywordId']
        keyword = row['keyword']
        targeting = row['targeting']
        matchType = row['matchType']
        adGroupName = row['adGroupName']
        campaignName = row['campaignName']
        ORDER_1m = row['ORDER_1m']
        total_clicks_30d = row['total_clicks_30d']
        total_clicks_7d = row['total_clicks_7d']
        total_clicks_yesterday = row['total_clicks_yesterday']
        total_sales14d_30d = row['total_sales14d_30d']
        total_sales14d_7d = row['total_sales14d_7d']
        total_sales14d_yesterday = row['total_sales14d_yesterday']
        total_cost_30d = row['total_cost_30d']
        total_cost_7d = row['total_cost_7d']
        total_cost_4d = row['total_cost_4d']
        total_cost_yesterday = row['total_cost_yesterday']
        ACOS_30d = row['ACOS_30d']
        ACOS_7d = row['ACOS_7d']
        ACOS_yesterday = row['ACOS_yesterday']
        keywordBid = row['keywordBid']

        # 定义一个变量来存放调整后的竞价信息或者关闭原因
        new_keywordBid = keywordBid
        reason = ""

        # 判断定义一
        if 0.24 < ACOS_7d <= 0.5 and 0 < ACOS_30d <= 0.5:
            new_keywordBid = keywordBid / ((ACOS_7d - 0.24) / 0.24 + 1)
            reason = "降低竞价"

        # 判断定义二
        elif ACOS_7d > 0.5 and ACOS_30d <= 0.36:
            new_keywordBid = keywordBid / ((ACOS_7d - 0.24) / 0.24 + 1)
            reason = "降低竞价"

        # 判断定义三
        elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and ACOS_30d <= 0.36:
            new_keywordBid = keywordBid - 0.04
            reason = "降低竞价0.04"

        # 判断定义四
        elif total_clicks_7d > 10 and total_sales14d_7d == 0 and ACOS_30d > 0.5:
            new_keywordBid = "关闭"
            reason = "关闭该词"

        # 判断定义五
        elif ACOS_7d > 0.5 and ACOS_30d > 0.36:
            new_keywordBid = "关闭"
            reason = "关闭该词"

        # 判断定义六
        elif total_sales14d_30d == 0 and total_cost_30d >= 5:
            new_keywordBid = "关闭"
            reason = "关闭该词"

        # 判断定义七
        elif total_sales14d_30d == 0 and total_clicks_30d >= 15 and total_clicks_7d > 0:
            new_keywordBid = "关闭"
            reason = "关闭该词"

        # 如果不为空且有调整，添加到结果
        if reason:
            results.append({
                "keyword": keyword,
                "keywordId": keywordId,
                "campaignName": campaignName,
                "adGroupName": adGroupName,
                "matchType": matchType,
                "keywordBid": keywordBid,
                "new_keywordBid": new_keywordBid,
                "targeting": targeting,
                "total_cost_7d": total_cost_7d,
                "total_sales14d_7d": total_sales14d_7d,
                "total_clicks_30d": total_clicks_30d,
                "ACOS_7d": ACOS_7d,
                "ACOS_30d": ACOS_30d,
                "reason": reason
            })

    return pd.DataFrame(results)

# 进行数据分析
results_df = analyze_ads(data)

# 保存结果到新的CSV文件
output_file = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/提问策略/手动_ASIN_劣质商品投放_v1_1_LAPASA_UK_2024-07-02.csv'
results_df.to_csv(output_file, index=False)

# 输出操作完成的信息
print("分析完成，结果已保存在{}".format(output_file))