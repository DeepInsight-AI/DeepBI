# filename: keyword_bid_adjustment.py

import pandas as pd

# 读取CSV文件
csv_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv"
df = pd.read_csv(csv_file_path)

# 初始化保存结果的列表
results = []

# 遍历每行以进行判断
for _, row in df.iterrows():
    keyword_id = row['keywordId']
    keyword = row['keyword']
    ad_group = row['adGroupName']
    campaign = row['campaignName']
    keyword_bid = row['keywordBid']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    clicks_7d = row['total_clicks_7d']
    clicks_30d = row['total_clicks_30d']
    sales_7d = row['total_sales14d_7d']
    sales_30d = row['total_sales14d_30d']
    orders_1m = row['ORDER_1m']

    new_keyword_bid = keyword_bid
    adjustment_reason = None

    # 定义一
    if 0.24 < acos_7d < 0.5 and 0 < acos_30d < 0.24:
        new_keyword_bid -= 0.03
        adjustment_reason = "定义一"

    # 定义二
    elif 0.24 < acos_7d < 0.5 and 0.24 < acos_30d < 0.5:
        new_keyword_bid -= 0.04
        adjustment_reason = "定义二"

    # 定义三
    elif sales_7d == 0 and clicks_7d > 0 and 0.24 < acos_30d < 0.5:
        new_keyword_bid -= 0.04
        adjustment_reason = "定义三"

    # 定义四
    elif 0.24 < acos_7d < 0.5 and acos_30d > 0.5:
        new_keyword_bid -= 0.05
        adjustment_reason = "定义四"

    # 定义五
    elif acos_7d > 0.5 and 0 < acos_30d < 0.24:
        new_keyword_bid -= 0.05
        adjustment_reason = "定义五"

    # 定义六
    elif sales_7d == 0 and clicks_7d > 0 and sales_30d == 0 and clicks_30d > 10:
        new_keyword_bid = "关闭"
        adjustment_reason = "定义六"

    # 定义七
    elif sales_7d == 0 and clicks_7d > 0 and acos_30d > 0.5:
        new_keyword_bid = "关闭"
        adjustment_reason = "定义七"

    # 定义八
    elif acos_7d > 0.5 and acos_30d > 0.24:
        new_keyword_bid = "关闭"
        adjustment_reason = "定义八"

    if adjustment_reason:
        results.append({
            "campaignName": campaign,
            "adGroupName": ad_group,
            "keyword": keyword,
            "keywordBid": keyword_bid,
            "New_keywordBid": new_keyword_bid,
            "ACOS_30d": acos_30d,
            "ACOS_7d": acos_7d,
            "clicks_7d": clicks_7d,
            "adjustment_amount": keyword_bid - new_keyword_bid if new_keyword_bid != "关闭" else "N/A",
            "adjustment_reason": adjustment_reason
        })

# 保存结果到新的CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_11_IT_2024-06-19.csv"
result_df = pd.DataFrame(results)
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("处理完成，结果已保存到:", output_file_path)