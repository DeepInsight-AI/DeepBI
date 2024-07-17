# filename: manual_ads_optimization.py

import pandas as pd

# 加载数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(file_path)

# 定义新的出价计算函数
def calculate_new_bid(row):
    if (0.24 < row['ACOS_7d'] <= 0.5) and (0 < row['ACOS_30d'] <= 0.5):
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] <= 0.36):
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif (row['total_clicks_7d'] >= 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] <= 0.36):
        return row['keywordBid'] - 0.04
    else:
        return row['keywordBid']

# 添加新列
df['new_keywordBid'] = df.apply(lambda row: calculate_new_bid(row), axis=1)
df['Action'] = df.apply(lambda row: '关闭' if (
    ((row['total_clicks_7d'] >= 10) and (row['total_sales14d_7d'] == 0) and (row['ACOS_30d'] > 0.5)) or 
    ((row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] > 0.36)) or 
    ((row['total_sales14d_30d'] == 0) and (row['total_cost_30d'] >= 5)) or 
    ((row['total_sales14d_30d'] == 0) and (row['total_clicks_30d'] >= 15) and (row['total_clicks_7d'] > 0)) 
) else '', axis=1)

# 对关闭的商品投放，设置new_keywordBid为'关闭'
df.loc[df['Action'] == '关闭', 'new_keywordBid'] = '关闭'

# 筛选需要输出的列和条件
output_df = df[(df['new_keywordBid'] != df['keywordBid']) | (df['Action'] == '关闭')]
output_columns = [
    "keyword", "keywordId", "campaignName", "adGroupName", "matchType", 
    "keywordBid", "new_keywordBid", "targeting", "total_cost_30d", 
    "total_clicks_30d", "total_clicks_7d", "total_sales14d_7d", 
    "total_cost_7d", "total_cost_4d", "ACOS_7d", "ACOS_30d", "Action"
]
output_df = output_df[output_columns]

# 保存结果到CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_IT_2024-06-30.csv"
output_df.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")