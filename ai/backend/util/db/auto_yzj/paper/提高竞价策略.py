# filename: 提高竞价策略.py
import pandas as pd

# 读取CSV文件
df = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv')

# 筛选符合定义一到定义六的商品投放信息
def compute_new_bid(row):
    bid_adjustment_reason = ''
    bid_increase = 0
    
    if (0 < row['ACOS_7d'] <= 0.1) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        bid_increase = 0.05
        bid_adjustment_reason = '满足定义一'
    elif (0 < row['ACOS_7d'] <= 0.1) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        bid_increase = 0.03
        bid_adjustment_reason = '满足定义二'
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        bid_increase = 0.04
        bid_adjustment_reason = '满足定义三'
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        bid_increase = 0.02
        bid_adjustment_reason = '满足定义四'
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        bid_increase = 0.02
        bid_adjustment_reason = '满足定义五'
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        bid_increase = 0.01
        bid_adjustment_reason = '满足定义六'

    return row['keywordBid'] + bid_increase, bid_increase, bid_adjustment_reason

results = []

# 计算新的竞价和提价原因
for index, row in df.iterrows():
    new_bid, increase, reason = compute_new_bid(row)
    
    if reason:  # 只有符合定义的商品投放才进行记录
        result = {
            'keyword': row['keyword'],
            'keywordId': row['keywordId'],
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            '匹配类型': row['matchType'],
            '商品投放出价(keywordBid)': row['keywordBid'],
            'New_keywordBid': new_bid,
            'targeting': row['targeting'],
            'cost': row['total_cost_30d'],
            'clicks': row['total_clicks_30d'],
            '最近7天的平均ACOS值': row['ACOS_7d'],
            '最近一个月的平均ACOS值': row['ACOS_30d'],
            '最近一个月的订单数': row['ORDER_1m'],
            '对该词进行提价多少': increase,
            '提价原因': reason
        }
        results.append(result)

# 转换结果为DataFrame
output_df = pd.DataFrame(results)

# 保存结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_DE_2024-07-03.csv'
output_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print("Results saved to:", output_path)