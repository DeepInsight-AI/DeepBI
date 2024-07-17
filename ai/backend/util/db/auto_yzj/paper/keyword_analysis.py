# filename: keyword_analysis.py

import pandas as pd

# 读取数据
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(data_path)

# 定义劣质关键词筛选函数
def adjust_bid(row, condition, reduction=0, minimal_bid=0.05):
    """
    根据给定的条件和减价额调整竞价
    """
    current_bid = row['keywordBid']
    adjusted_bid = current_bid / ((row['ACOS_7d'] - 0.24) / 0.24 + 1) if reduction == 0 else current_bid - reduction
    return max(minimal_bid, adjusted_bid)

reasons = []
new_bids = []

for i, row in df.iterrows():
    if row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.5 and row['ORDER_1m'] < 5 and row['ACOS_3d'] >= 0.24:
        new_bids.append(adjust_bid(row, condition=1))
        reasons.append('定义一')
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36 and row['ACOS_3d'] > 0.24:
        new_bids.append(adjust_bid(row, condition=2))
        reasons.append('定义二')
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] <= 5 and row['ACOS_30d'] <= 0.36:
        new_bids.append(adjust_bid(row, condition=3, reduction=0.03))
        reasons.append('定义三')
    elif row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] > 7 and row['ACOS_30d'] > 0.5:
        new_bids.append(0.05 if row['keywordBid'] >= 0.05 else row['keywordBid'])
        reasons.append('定义四')
    elif row['ACOS_7d'] > 0.5 and row['ACOS_3d'] > 0.24 and row['ACOS_30d'] > 0.36:
        new_bids.append(0.05 if row['keywordBid'] >= 0.05 else row['keywordBid'])
        reasons.append('定义五')
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 10 and row['total_clicks_30d'] >= 15:
        new_bids.append(0.05 if row['keywordBid'] >= 0.05 else row['keywordBid'])
        reasons.append('定义六')
    elif row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.5 and row['ORDER_1m'] < 5 and row['total_sales14d_3d'] == 0:
        new_bids.append(adjust_bid(row, condition=7))
        reasons.append('定义七')
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36 and row['total_sales14d_3d'] == 0:
        new_bids.append(adjust_bid(row, condition=8))
        reasons.append('定义八')
    elif row['ACOS_7d'] > 0.5 and row['total_sales14d_3d'] == 0 and row['ACOS_30d'] > 0.36:
        new_bids.append(0.05 if row['keywordBid'] >= 0.05 else row['keywordBid'])
        reasons.append('定义九')
    elif row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0.5 and row['ORDER_1m'] < 5 and row['ACOS_3d'] >= 0.24:
        new_bids.append(adjust_bid(row, condition=10))
        reasons.append('定义十')
    elif row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['total_sales14d_3d'] == 0 and row['ACOS_30d'] > 0.5:
        new_bids.append(adjust_bid(row, condition=11))
        reasons.append('定义十一')
    elif row['ACOS_7d'] <= 0.24 and row['total_sales14d_3d'] == 0 and 3 < row['total_cost_3d'] < 5:
        new_bids.append(adjust_bid(row, condition=12, reduction=0.01))
        reasons.append('定义十二')
    elif row['ACOS_7d'] <= 0.24 and 0.24 < row['ACOS_3d'] < 0.36:
        new_bids.append(adjust_bid(row, condition=13, reduction=0.02))
        reasons.append('定义十三')
    elif row['ACOS_7d'] <= 0.24 and row['ACOS_3d'] > 0.36:
        new_bids.append(adjust_bid(row, condition=14, reduction=0.03))
        reasons.append('定义十四')
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] >= 10 and row['ACOS_30d'] <= 0.36:
        new_bids.append(0.05 if row['keywordBid'] >= 0.05 else row['keywordBid'])
        reasons.append('定义十五')
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and 5 < row['total_cost_7d'] < 10 and row['ACOS_30d'] <= 0.36:
        new_bids.append(adjust_bid(row, condition=16, reduction=0.07))
        reasons.append('定义十六')
    else:
        new_bids.append(row['keywordBid'])
        reasons.append('')

df['new_keywordBid'] = new_bids
df['reason'] = reasons

result = df[df['reason'] != '']

# 保存结果
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_IT_2024-07-15.csv'
result.to_csv(output_path, index=False)
print(f"结果已保存到 {output_path}")