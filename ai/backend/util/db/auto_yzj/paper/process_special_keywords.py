# filename: process_special_keywords.py

import pandas as pd

# 指定CSV文件路径
input_csv_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/特殊关键词/预处理.csv"
output_csv_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/特殊关键词/提问策略/手动_特殊关键词_ES_2024-06-10.csv"

# 读取CSV文件
df = pd.read_csv(input_csv_path)

# 校验需要的列是否存在
required_columns = [
    'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d',
    'keyword', 'matchType', 'keywordBid', 'keywordId'
]
for column in required_columns:
    if column not in df.columns:
        raise ValueError(f"Missing required column: {column}")

# 获取广告组中的所有广告组名称
ad_groups = df['adGroupName'].unique()

# 准备空的DataFrame以存储最终结果
result_df = pd.DataFrame()

# 遍历每个广告组名称
for ad_group in ad_groups:
    # 提取当前广告组的数据
    ad_group_df = df[df['adGroupName'] == ad_group]
    
    # 检查广告组的15天总销售额和所有关键词的7天总点击次数
    if (ad_group_df['total_sales_15d'].iloc[0] == 0 
            and ad_group_df['total_clicks_7d'].sum() <= 12):
        
        # 调整当前广告组中的所有关键词的竞价
        ad_group_df['new_bid'] = ad_group_df['keywordBid'] + 0.02
        ad_group_df['reason'] = 'No sales in last 15 days and total clicks <= 12 in last 7 days'
        
        # 将修改后的数据添加到结果DataFrame中
        result_df = pd.concat([result_df, ad_group_df], ignore_index=True)

# 确保所有需要的列在结果DataFrame中
result_columns = [
    'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d',
    'keyword', 'matchType', 'keywordBid', 'keywordId', 'new_bid', 'reason'
]
for column in result_columns:
    if column not in result_df.columns:
        result_df[column] = None  # 添加缺失列并填充值为None

# 保存结果到CSV文件
result_df.to_csv(output_csv_path, columns=result_columns, index=False)

print("Processing complete. Results saved to:", output_csv_path)