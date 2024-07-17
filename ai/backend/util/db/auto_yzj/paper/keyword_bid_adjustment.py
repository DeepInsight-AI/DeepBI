# filename: keyword_bid_adjustment.py

import pandas as pd

# 读取数据
filepath = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
data = pd.read_csv(filepath)

# 定义多条判断规则的函数
def adjust_keyword_bid(row):
    keywordBid = row['keywordBid']
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    avg_ACOS_3d = row['ACOS_3d']
    cost_7d = row['total_cost_7d']
    cost_30d = row['total_cost_30d']
    cost_3d = row['total_cost_3d']
    clicks_7d = row['total_clicks_7d']
    sales_7d = row['total_sales14d_7d']
    sales_3d = row['total_sales14d_3d']
    total_sales_30d = row['total_sales14d_30d']
    clicks_30d = row['total_clicks_30d']
    orders_1m = row['ORDER_1m']
    
    new_keywordBid = keywordBid  # 默认新竞价为当前竞价
    action = ""
    
    # 定义一
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and orders_1m < 5 and avg_ACOS_3d >= 0.24:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "降低竞价"
    # 定义二
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d >= 0.24:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "降低竞价"
    # 定义三
    elif clicks_7d >= 10 and sales_7d == 0 and cost_7d <= 5 and avg_ACOS_30d <= 0.36:
        new_keywordBid = keywordBid - 0.03
        action = "降低竞价0.03"
    # 定义四
    elif clicks_7d > 10 and sales_7d == 0 and cost_7d > 7 and avg_ACOS_30d > 0.5:
        new_keywordBid = 0.05
        action = "将竞价设为0.05"
    # 定义五
    elif avg_ACOS_7d > 0.5 and avg_ACOS_3d >= 0.24 and avg_ACOS_30d > 0.36:
        new_keywordBid = 0.05
        action = "将竞价设为0.05"
    # 定义六
    elif total_sales_30d == 0 and cost_30d >= 10 and clicks_30d >= 15:
        new_keywordBid = 0.05
        action = "将竞价设为0.05"
    # 定义七
    elif 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and orders_1m < 5 and sales_3d == 0:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "降低竞价"
    # 定义八
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and sales_3d == 0:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "降低竞价"
    # 定义九
    elif avg_ACOS_7d > 0.5 and sales_3d == 0 and avg_ACOS_30d > 0.36:
        new_keywordBid = 0.05
        action = "将竞价设为0.05"
    # 定义十
    elif 0.24 < avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0.5 and orders_1m < 5 and avg_ACOS_3d >= 0.24:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "降低竞价"
    # 定义十一
    elif 0.24 < avg_ACOS_7d <= 0.5 and sales_3d == 0 and avg_ACOS_30d > 0.5:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        action = "降低竞价"
    # 定义十二
    elif avg_ACOS_7d <= 0.24 and sales_3d == 0 and 3 < cost_3d < 5:
        new_keywordBid = keywordBid - 0.01
        action = "降低竞价0.01"
    # 定义十三
    elif avg_ACOS_7d <= 0.24 and 0.24 < avg_ACOS_3d < 0.36:
        new_keywordBid = keywordBid - 0.02
        action = "降低竞价0.02"
    # 定义十四
    elif avg_ACOS_7d <= 0.24 and avg_ACOS_3d > 0.36:
        new_keywordBid = keywordBid - 0.03
        action = "降低竞价0.03"
    # 定义十五
    elif clicks_7d >= 10 and sales_7d == 0 and cost_7d >= 10 and avg_ACOS_30d <= 0.36:
        new_keywordBid = 0.05
        action = "将竞价设为0.05"
    # 定义十六
    elif clicks_7d >= 10 and sales_7d == 0 and 5 < cost_7d < 10 and avg_ACOS_30d <= 0.36:
        new_keywordBid = keywordBid - 0.07
        action = "降低竞价0.07"

    # 限制竞价下限为0.05
    if new_keywordBid < 0.05:
        new_keywordBid = keywordBid
        action = "竞价保持不变"

    return new_keywordBid, action

# 调整竞价并生成结果列表
results = []
for index, row in data.iterrows():
    new_bid, reason = adjust_keyword_bid(row)
    results.append([
        row['keyword'],
        row['keywordId'],
        row['campaignName'],
        row['adGroupName'],
        row['matchType'],
        row['keywordBid'],
        new_bid,
        row['targeting'],
        row['total_cost_yesterday'],
        row['total_clicks_7d'],
        row['total_cost_7d'],
        row['total_sales14d_7d'],
        None,  # 广告组最近7天的总花费（需要原始数据集中的字段或计算方式）
        row['ACOS_7d'],
        row['ACOS_30d'],
        row['ACOS_3d'],
        row['total_clicks_30d'],
        reason
    ])

# 创建结果数据帧并保存为CSV
results_df = pd.DataFrame(results, columns=[
    'keyword',
    'keywordId',
    'campaignName',
    'adGroupName',
    'matchType',
    'keywordBid',
    'new_keywordBid',
    'targeting',
    'cost_yesterday',
    'clicks_7d',
    'cost_7d',
    'sales_7d',
    'adGroup_cost_7d',
    'acos_7d',
    'acos_30d',
    'acos_3d',
    'clicks_30d',
    'reason'
])

output_filepath = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-14.csv"
results_df.to_csv(output_filepath, index=False, encoding='utf-8-sig')

print("关键词竞价调整结果已保存至:", output_filepath)