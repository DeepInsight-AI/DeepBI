# filename: adjust_keyword_bids.py

import pandas as pd

# 加载CSV文件
data_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
data = pd.read_csv(data_path)

# 定义判定和处理逻辑的函数
def adjust_bid(row):
    keyword_bid = row['keywordBid']
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    avg_ACOS_3d = row['ACOS_3d']
    clicks_7d = row['total_clicks_7d']
    cost_7d = row['total_cost_7d']
    sales_7d = row['total_sales14d_7d']
    sales_3d = row['total_sales14d_3d']
    cost_3d = row['total_cost_3d']
    clicks_30d = row['total_clicks_30d']
    sales_30d = row['total_sales14d_30d']
    orders_30d = row['ORDER_1m']
    cost_30d = row['total_cost_30d']

    # 初始值
    new_keyword_bid = keyword_bid
    action_reason = '未定义'  # 初始化为未定义

    # 定义一
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and orders_30d < 5 and avg_ACOS_3d >= 0.24 and sales_3d > 0:
        new_keyword_bid = max(0.05, keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1))
        action_reason = '定义一'

    # 定义二
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d >= 0.24 and sales_3d > 0:
        new_keyword_bid = max(0.05, keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1))
        action_reason = '定义二'

    # 定义三
    elif clicks_7d >= 10 and sales_7d == 0 and cost_7d <= 5 and avg_ACOS_30d <= 0.36:
        new_keyword_bid = max(0.05, keyword_bid - 0.03)
        action_reason = '定义三'

    # 定义四
    elif clicks_7d > 10 and sales_7d == 0 and cost_7d > 7 and avg_ACOS_30d > 0.5:
        new_keyword_bid = max(0.05, 0.05)
        action_reason = '定义四'

    # 定义五
    elif avg_ACOS_7d > 0.5 and avg_ACOS_3d >= 0.24 and avg_ACOS_30d > 0.36:
        new_keyword_bid = max(0.05, 0.05)
        action_reason = '定义五'

    # 定义六
    elif sales_30d == 0 and cost_30d >= 10 and clicks_30d >= 15:
        new_keyword_bid = max(0.05, 0.05)
        action_reason = '定义六'
    
    # 定义七
    elif 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and orders_30d < 5 and avg_ACOS_3d == 0:
        new_keyword_bid = max(0.05, keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1))
        action_reason = '定义七'
    
    # 定义八
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d == 0:
        new_keyword_bid = max(0.05, keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1))
        action_reason = '定义八'
    
    # 定义九
    elif avg_ACOS_7d > 0.5 and avg_ACOS_3d == 0 and avg_ACOS_30d > 0.36:
        new_keyword_bid = max(0.05, 0.05)
        action_reason = '定义九'
    
    # 定义十
    elif 0.24 < avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0.5 and orders_30d < 5 and avg_ACOS_3d >= 0.24:
        new_keyword_bid = max(0.05, keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1))
        action_reason = '定义十'
    
    # 定义十一
    elif 0.24 < avg_ACOS_7d <= 0.5 and sales_3d == 0 and avg_ACOS_30d > 0.5:
        new_keyword_bid = max(0.05, keyword_bid / ((avg_ACOS_7d - 0.24) / 0.24 + 1))
        action_reason = '定义十一'
    
    # 定义十二
    elif avg_ACOS_7d <= 0.24 and sales_3d == 0 and 3 < cost_3d < 5:
        new_keyword_bid = max(0.05, keyword_bid - 0.01)
        action_reason = '定义十二'
    
    # 定义十三
    elif avg_ACOS_7d <= 0.24 and 0.24 < avg_ACOS_3d < 0.36:
        new_keyword_bid = max(0.05, keyword_bid - 0.02)
        action_reason = '定义十三'
    
    # 定义十四
    elif avg_ACOS_7d <= 0.24 and avg_ACOS_3d > 0.36:
        new_keyword_bid = max(0.05, keyword_bid - 0.03)
        action_reason = '定义十四'
    
    # 定义十五
    elif clicks_7d >= 10 and sales_7d == 0 and cost_7d >= 10 and avg_ACOS_30d <= 0.36:
        new_keyword_bid = max(0.05, 0.05)
        action_reason = '定义十五'
    
    # 定义十六
    elif clicks_7d >= 10 and sales_7d == 0 and 5 < cost_7d < 10 and avg_ACOS_30d <= 0.36:
        new_keyword_bid = max(0.05, keyword_bid - 0.07)
        action_reason = '定义十六'

    # 返回调整后的竞价及原因
    return new_keyword_bid, action_reason

# 应用函数并得到结果
data[['new_keywordBid', 'action_reason']] = data.apply(adjust_bid, axis=1, result_type='expand')

# 输出结果到CSV文件
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_FR_2024-07-15.csv"
data.to_csv(output_path, index=False)

print("Adjustments completed and saved to", output_path)