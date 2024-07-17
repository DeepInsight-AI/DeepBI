# filename: process_advertisement.py
import pandas as pd

# 路径和文件名
input_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_ES_2024-07-10.csv"

# 读取CSV文件
df = pd.read_csv(input_file_path)

# 计算每个广告组最近15天的总销售额和最近7天的总点击次数
ad_group_sales = df.groupby('adGroupName')['total_sales_15d'].sum().reset_index()
ad_group_clicks = df.groupby('adGroupName')['total_clicks_7d'].max().reset_index()

# 筛选出最近15天总销售额为0 并且 最近7天总点击次数小于等于12的广告组
poor_performance_ad_groups = ad_group_sales[(ad_group_sales['total_sales_15d'] == 0) & 
                                            (ad_group_clicks['total_clicks_7d'] <= 12)]

# 筛选出这些广告组中的所有商品投放并提高其竞价0.02
results = df[df['adGroupName'].isin(poor_performance_ad_groups['adGroupName'])]
results['new_keywordBid'] = results['keywordBid'] + 0.02
results['原因'] = '广告组15天总销售额为0且所有商品7天点击次数<=12'

# 按需选择特定列并保存结果到新的CSV文件
results = results[['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 
                   'matchType', 'keywordBid', 'keywordId', 'new_keywordBid', '原因']]
results.to_csv(output_file_path, index=False)

print(f"处理完成，结果保存在：{output_file_path}")