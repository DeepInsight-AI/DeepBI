# filename: process_ads_bid.py
import pandas as pd

# 设置文件路径
input_filepath = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/特殊商品投放/预处理.csv"
output_filepath = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/特殊商品投放/提问策略/手动_ASIN_特殊商品投放_v1_1_DELOMO_ES_2024-07-09.csv"

# 读取CSV文件
df = pd.read_csv(input_filepath)

# 过滤掉15天内总销售额为0的广告组
df_filtered = df[df['total_sales_15d'] == 0]

# 找到广告组里所有商品投放最近7天的总点击次数小于等于12的广告组
results = []
for ad_group_name, group in df_filtered.groupby('adGroupName'):
    if (group['total_clicks_7d'] <= 12).all():
        for _, row in group.iterrows():
            new_row = row.copy()
            new_row['new_keywordBid'] = new_row['keywordBid'] + 0.02
            new_row['reason'] = '15天总销售额为0且7天总点击次数小于等于12，增加竞价0.02'
            results.append(new_row)

# 将结果转换为新的DataFrame
result_df = pd.DataFrame(results)

# 选择需要输出的字段
output_fields = [
    'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 
    'keyword', 'matchType', 'keywordBid', 'keywordId', 'new_keywordBid', 'reason'
]

# 保存到CSV文件
result_df.to_csv(output_filepath, columns=output_fields, index=False)
print("结果已保存到指定文件.")