# filename: process_poor_keywords.py
import pandas as pd

# 读取数据集
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv"
data = pd.read_csv(file_path)

# 初始化结果列表
results = []

# 遍历每行数据进行判定
for index, row in data.iterrows():
    keywordId = row['keywordId']
    keyword = row['keyword']
    targeting = row['targeting']
    keywordBid = row['keywordBid']
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

    new_keywordBid = keywordBid
    action = ""
    reason = ""

    # 定义一
    if 0.24 < ACOS_7d <= 0.5 and 0 < ACOS_30d <= 0.5:
        new_keywordBid = keywordBid / (((ACOS_7d - 0.24) / 0.24) + 1)
        action = "调整竞价"
        reason = "定义一：7天和30天ACOS分别在[0.24, 0.5]和(0, 0.5]区间"

    # 定义二
    elif ACOS_7d > 0.5 and ACOS_30d <= 0.36:
        new_keywordBid = keywordBid / (((ACOS_7d - 0.24) / 0.24) + 1)
        action = "调整竞价"
        reason = "定义二：7天ACOS > 0.5 和 30天ACOS <= 0.36"

    # 定义三
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and ACOS_30d <= 0.36:
        new_keywordBid = max(0, keywordBid - 0.04)
        action = "降低竞价"
        reason = "定义三：7天点击数 >= 10, 7天销售额 = 0, 30天ACOS <= 0.36"

    # 定义四
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and ACOS_30d > 0.5:
        new_keywordBid = "关闭"
        action = "关闭关键词"
        reason = "定义四：7天点击数 >= 10, 7天销售额 = 0, 30天ACOS > 0.5"

    # 定义五
    elif ACOS_7d > 0.5 and ACOS_30d > 0.36:
        new_keywordBid = "关闭"
        action = "关闭关键词"
        reason = "定义五：7天ACOS > 0.5 和 30天ACOS > 0.36"

    # 定义六
    elif total_sales14d_30d == 0 and total_cost_7d > (total_cost_yesterday * 7) / 5:
        new_keywordBid = "关闭"
        action = "关闭关键词"
        reason = "定义六：30天销售额 = 0, 7天花费 > 广告组最近7天花费的1/5"

    # 定义七
    elif total_sales14d_30d == 0 and total_clicks_30d >= 13:
        new_keywordBid = "关闭"
        action = "关闭关键词"
        reason = "定义七：30天销售额 = 0, 30天点击数 >= 13"

    if action:
        results.append({
            'keyword': keyword,
            'keywordId': keywordId,
            'campaignName': campaignName,
            'adGroupName': adGroupName,
            'matchType': matchType,
            'keywordBid': keywordBid,
            'new_keywordBid': new_keywordBid,
            'targeting': targeting,
            'cost': total_cost_30d,
            'clicks': total_clicks_7d,
            'total_cost_7d': total_cost_7d,
            'total_sales14d_7d': total_sales14d_7d,
            'total_cost_yesterday': total_cost_yesterday,
            'ACOS_7d': ACOS_7d,
            'ACOS_30d': ACOS_30d,
            'action': action,
            'reason': reason
        })

# 将结果保存到CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_劣质关键词_v1_1_ES_2024-06-14.csv"
results_df = pd.DataFrame(results)
results_df.to_csv(output_file_path, index=False, encoding="utf-8-sig")

print("处理完成，结果保存在:", output_file_path)