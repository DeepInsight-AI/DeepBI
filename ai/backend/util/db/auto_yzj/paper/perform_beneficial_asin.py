# filename: perform_beneficial_asin.py

import pandas as pd

# Step 1: 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: 定义提升竞价的函数
def update_bid(row, increase, reason):
    new_bid = row['keywordBid'] + increase
    return pd.Series({
        'keyword': row['keyword'],
        'keywordId': row['keywordId'],
        'campaignName': row['campaignName'],
        'adGroupName': row['adGroupName'],
        'matchType': row['matchType'],
        'keywordBid': row['keywordBid'],
        'New_keywordBid': new_bid,
        'targeting': row['targeting'],
        'cost': row['total_cost_30d'],
        'clicks': row['total_clicks_7d'],
        'recent_ACOS_7d': row['ACOS_7d'],
        'recent_ACOS_30d': row['ACOS_30d'],
        'order_count_1m': row['ORDER_1m'],
        'bid_increase': increase,
        'raise_reason': reason
    })

# Step 3: 筛选并构建新的 DataFrame
beneficial_asins = []

for _, row in data.iterrows():
    if (0 < row['ACOS_7d'] <= 0.1) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        beneficial_asins.append(update_bid(row, 0.05, 'Definition 1'))
    elif (0 < row['ACOS_7d'] <= 0.1) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        beneficial_asins.append(update_bid(row, 0.03, 'Definition 2'))
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        beneficial_asins.append(update_bid(row, 0.04, 'Definition 3'))
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        beneficial_asins.append(update_bid(row, 0.02, 'Definition 4'))
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        beneficial_asins.append(update_bid(row, 0.02, 'Definition 5'))
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        beneficial_asins.append(update_bid(row, 0.01, 'Definition 6'))

# 构建新的 DataFrame
result_df = pd.DataFrame(beneficial_asins)

# Step 4: 输出到新的 CSV 文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_ITES_2024-07-02.csv'
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("任务完成，文件已保存到：", output_file_path)