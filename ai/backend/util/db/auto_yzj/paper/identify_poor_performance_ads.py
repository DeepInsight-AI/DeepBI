# filename: identify_poor_performance_ads.py

import pandas as pd

# 读取csv文件路径
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"

# 读取数据
data = pd.read_csv(file_path)

# 按照定义条件筛选表现较差的商品投放
poor_ads = []

for index, row in data.iterrows():
    new_bid = row['keywordBid']  # 默认不变
    
    if (0.24 < row['ACOS_7d'] <= 0.5) and (0 < row['ACOS_30d'] <= 0.5):
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = '定义一'
    
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] <= 0.36):
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = '定义二'

    elif (row['total_clicks_7d'] >= 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] <= 0.36):
        new_bid = row['keywordBid'] - 0.04
        reason = '定义三'

    elif (row['total_clicks_7d'] > 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] > 0.5):
        new_bid = '关闭'
        reason = '定义四'

    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] > 0.36):
        new_bid = '关闭'
        reason = '定义五'

    elif (row['total_sales14d_30d'] == 0) and (row['total_cost_30d'] >= 5):
        new_bid = '关闭'
        reason = '定义六'

    elif (row['total_sales14d_30d'] == 0) and (row['total_clicks_30d'] >= 15) and (row['total_clicks_7d'] > 0):
        new_bid = '关闭'
        reason = '定义七'

    else:
        continue
    
    poor_ads.append({
        'keyword': row['keyword'],
        'keywordId': row['keywordId'],
        'campaignName': row['campaignName'],
        'adGroupName': row['adGroupName'],
        'matchType': row['matchType'],
        'keywordBid': row['keywordBid'],
        'New_keywordBid': new_bid,
        'targeting': row['targeting'],
        'total_cost_30d': row['total_cost_30d'],
        'total_clicks_30d': row['total_clicks_30d'],
        'total_cost_7d': row['total_cost_7d'],
        'total_sales14d_7d': row['total_sales14d_7d'],
        'ACOS_7d': row['ACOS_7d'],
        'ACOS_30d': row['ACOS_30d'],
        'reason': reason
    })
    
# 转换为DataFrame并保存新的CSV文件
result_df = pd.DataFrame(poor_ads)
result_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_FR_2024-07-09.csv"
result_df.to_csv(result_file_path, index=False)

print(f"结果已保存至 {result_file_path}")