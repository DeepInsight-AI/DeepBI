# filename: auto_positioning_price_reduction.py

import pandas as pd

# 读取CSV文件
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/自动定位组优化/预处理.csv'
data = pd.read_csv(file_path)

# 筛选出符合条件的记录并添加降价原因
results = []

for index, row in data.iterrows():
    ACOS_30d = row['ACOS_30d']
    ACOS_7d = row['ACOS_7d']
    keywordBid = row['keywordBid']
    total_clicks_7d = row['total_clicks_7d']
    total_sales14d_7d = row['total_sales14d_7d']

    if 0.24 < ACOS_7d < 0.5:
        if 0 < ACOS_30d < 0.24:
            reason = "定义一"
            new_bid = keywordBid - 0.03
        elif 0.24 < ACOS_30d < 0.5:
            reason = "定义二"
            new_bid = keywordBid - 0.04
        elif ACOS_30d > 0.5:
            reason = "定义四"
            new_bid = keywordBid - 0.05
    elif ACOS_7d > 0.5 and 0 < ACOS_30d < 0.24:
        reason = "定义五"
        new_bid = keywordBid - 0.05
    elif total_sales14d_7d == 0 and total_clicks_7d > 0 and 0.24 < ACOS_30d < 0.5:
        reason = "定义三"
        new_bid = keywordBid - 0.04
    else:
        continue  # 不符合以上条件则跳过

    results.append({
        'campaignName': row['campaignName'],
        'adGroupName': row['adGroupName'],
        'keyword': row['keyword'],
        'ACOS_30d': ACOS_30d,
        'ACOS_7d': ACOS_7d,
        '降价的原因': reason,
        '新竞价': new_bid
    })

# 结果转为DataFrame
results_df = pd.DataFrame(results)

# 保存到新的CSV文件
output_file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/自动定位组优化/提问策略/劣质自动定位组_FR.csv'
results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_file_path}")