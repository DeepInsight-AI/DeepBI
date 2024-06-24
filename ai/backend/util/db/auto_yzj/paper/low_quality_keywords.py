# filename: low_quality_keywords.py

import pandas as pd

# 读取数据
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/关键词优化/预处理.csv"
df = pd.read_csv(file_path, encoding='utf-8')

# 计算广告组最近7天的总花费
ad_group_7d_cost = df.groupby('adGroupName')['total_cost_7d'].sum().reset_index()
ad_group_7d_cost.columns = ['adGroupName', 'adGroup_cost_7d']

# 合并广告组的总花费到原数据集中
df = pd.merge(df, ad_group_7d_cost, on='adGroupName', how='left')

# 定义条件和处理函数
results = []

def process_keyword(row):
    new_bid = row['keywordBid']
    reason = None
    op_type = None
    
    # 定义一
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义一: 调低竞价"
        
    # 定义二
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义二: 调低竞价"
        
    # 定义三
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] - 0.04
        reason = "定义三: 调低竞价0.04"
    
    # 定义四
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        new_bid = "关闭"
        reason = "定义四: 关闭关键词"
    
    # 定义五
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        new_bid = "关闭"
        reason = "定义五: 关闭关键词"
    
    # 定义六
    elif row['total_sales14d_30d'] == 0 and row['total_cost_7d'] > row['adGroup_cost_7d'] / 5:
        new_bid = "关闭"
        reason = "定义六: 关闭关键词"
    
    # 定义七
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15:
        new_bid = "关闭"
        reason = "定义七: 关闭关键词"
    
    if reason:
        results.append({
            "keywordId": row["keywordId"],
            "keyword": row["keyword"],
            "matchType": row["matchType"],
            "keywordBid": row["keywordBid"],
            "new_keywordBid": new_bid,
            "targeting": row["targeting"],
            "total_cost_7d": row["total_cost_7d"],
            "total_sales14d_7d": row["total_sales14d_7d"],
            "adGroup_cost_7d": row["adGroup_cost_7d"],
            "ACOS_7d": row["ACOS_7d"],
            "ACOS_30d": row["ACOS_30d"],
            "reason": reason,
            "op_type": "调整竞价" if new_bid != "关闭" else "关闭关键词",
            "campaignName": row["campaignName"],
            "adGroupName": row["adGroupName"]
        })

df.apply(lambda row: process_keyword(row), axis=1)

# 转换为DataFrame
results_df = pd.DataFrame(results)

# 保存结果到CSV文件
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/关键词优化/提问策略/手动_劣质关键词_v1_1_ES_2024-06-12.csv"
results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"处理完毕，结果保存至 {output_file_path}")