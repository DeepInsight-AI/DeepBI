# filename: find_poor_performance_ads.py

import pandas as pd
import os

# 读取CSV文件
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(input_file, encoding='utf-8')

# 定义条件和计算新竞价函数
def calculate_new_bid(row):
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    keywordBid = row['keywordBid']
    
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5:
        return keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
    if avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36:
        return keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
    return None

def determine_action(row):
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    clicks_7d = row['total_clicks_7d']
    sales_7d = row['total_sales14d_7d']
    sales_30d = row['total_sales14d_30d']
    cost_30d = row['total_cost_30d']
    clicks_30d = row['total_clicks_30d']
    
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5:
        return {'New_keywordBid': calculate_new_bid(row), 'Reason': '定義一'}
    if avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36:
        return {'New_keywordBid': calculate_new_bid(row), 'Reason': '定義二'}
    if clicks_7d >= 10 and sales_7d == 0 and avg_ACOS_30d <= 0.36:
        return {'New_keywordBid': max(row['keywordBid'] - 0.04, 0), 'Reason': '定义三'}
    if clicks_7d > 10 and sales_7d == 0 and avg_ACOS_30d > 0.5:
        return {'New_keywordBid': '关闭', 'Reason': '定义四'}
    if avg_ACOS_7d > 0.5 and avg_ACOS_30d > 0.36:
        return {'New_keywordBid': '关闭', 'Reason': '定义五'}
    if sales_30d == 0 and cost_30d >= 5:
        return {'New_keywordBid': '关闭', 'Reason': '定义六'}
    if sales_30d == 0 and clicks_30d >= 15 and clicks_7d > 0:
        return {'New_keywordBid': '关闭', 'Reason': '定义七'}
    return None

# 操作步骤
def apply_optimizations(df):
    actions = []

    for index, row in df.iterrows():
        action = determine_action(row)
        if action:
            actions.append({
                'keyword': row['keyword'],
                'keywordId': row['keywordId'],
                'campaignName': row['campaignName'],
                'adGroupName': row['adGroupName'],
                'matchType': row['matchType'],
                'keywordBid': row['keywordBid'],
                'New_keywordBid': action['New_keywordBid'],
                'targeting': row['targeting'],
                'total_cost_yesterday': row['total_cost_yesterday'],
                'total_sales14d_yesterday': row['total_sales14d_yesterday'],
                'ACOS_7d': row['ACOS_7d'],
                'ACOS_30d': row['ACOS_30d'],
                'total_clicks_30d': row['total_clicks_30d'],
                'total_sales14d_30d': row['total_sales14d_30d'],
                'total_cost_7d': row['total_cost_7d'],
                'total_clicks_7d': row['total_clicks_7d'],
                'Reason': action['Reason']
            })

    return pd.DataFrame(actions)

output_df = apply_optimizations(df)

# 设置输出路径
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_ES_2024-06-30.csv'

# 保存输出结果
output_df.to_csv(output_file, index=False, encoding='utf-8')

print(f"优化结果已保存在 {output_file}")