# filename: process_poor_performance_campaigns.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(file_path)

# 定义函数来处理商品投放
def process_campaigns(df):
    results = []

    for index, row in df.iterrows():
        keyword = row['keyword']
        keywordId = row['keywordId']
        campaignName = row['campaignName']
        adGroupName = row['adGroupName']
        matchType = row['matchType']
        keywordBid = row['keywordBid']
        targeting = row['targeting']
        total_cost_7d = row['total_cost_7d']
        total_sales14d_7d = row['total_sales14d_7d']
        total_cost_30d = row['total_cost_30d']
        avg_ACOS_7d = row['ACOS_7d']
        avg_ACOS_30d = row['ACOS_30d']
        total_clicks_7d = row['total_clicks_7d']
        total_sales14d_30d = row['total_sales14d_30d']
        total_clicks_30d = row['total_clicks_30d']

        new_keywordBid = None
        reason = None

        # 定义一
        if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5:
            new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
            reason = '定义一'

        # 定义二
        elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36:
            new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
            reason = '定义二'
        
        # 定义三
        elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and avg_ACOS_30d <= 0.36:
            new_keywordBid = max(0, keywordBid - 0.04)
            reason = '定义三'
        
        # 定义四
        elif total_clicks_7d > 10 and total_sales14d_7d == 0 and avg_ACOS_30d > 0.5:
            new_keywordBid = '关闭'
            reason = '定义四'

        # 定义五
        elif avg_ACOS_7d > 0.5 and avg_ACOS_30d > 0.36:
            new_keywordBid = '关闭'
            reason = '定义五'

        # 定义六
        elif total_sales14d_30d == 0 and total_cost_30d >= 5:
            new_keywordBid = '关闭'
            reason = '定义六'

        # 定义七
        elif total_sales14d_30d == 0 and total_clicks_30d >= 15 and total_clicks_7d > 0:
            new_keywordBid = '关闭'
            reason = '定义七'

        if reason:
            results.append({
                'keyword': keyword,
                'keywordId': keywordId,
                'campaignName': campaignName,
                'adGroupName': adGroupName,
                'matchType': matchType,
                'keywordBid': keywordBid,
                'New_keywordBid': new_keywordBid,
                'targeting': targeting,
                'total_cost_7d': total_cost_7d,
                'total_sales14d_7d': total_sales14d_7d,
                'avg_ACOS_7d': avg_ACOS_7d,
                'avg_ACOS_30d': avg_ACOS_30d,
                'total_clicks_30d': total_clicks_30d,
                'reason': reason
            })

    return pd.DataFrame(results)

# 处理数据并生成结果
result_df = process_campaigns(df)

# 输出结果至CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_US_2024-07-09.csv"
result_df.to_csv(output_file_path, index=False)

print(f"Results saved to {output_file_path}")