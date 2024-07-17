# filename: adjust_bid_for_good_ad_positions_debug_v3.py
import pandas as pd

file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/广告位优化/预处理.csv"
output_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/广告位优化/提问策略/自动_优质广告位_v1_1_LAPASA_DE_2024-07-14.csv"

df = pd.read_csv(file_path)

# 字段预处理
df['bid'] = pd.to_numeric(df['bid'], errors='coerce')
df['total_clicks_3d'] = pd.to_numeric(df['total_clicks_3d'], errors='coerce')
df['total_clicks_7d'] = pd.to_numeric(df['total_clicks_7d'], errors='coerce')
df['total_cost_3d'] = pd.to_numeric(df['total_cost_3d'], errors='coerce')
df['ACOS_3d'] = pd.to_numeric(df['ACOS_3d'], errors='coerce')
df['ACOS_7d'] = pd.to_numeric(df['ACOS_7d'], errors='coerce')

df['bid'].fillna(0, inplace=True)
df['total_clicks_3d'].fillna(0, inplace=True)
df['total_clicks_7d'].fillna(0, inplace=True)
df['total_cost_3d'].fillna(0, inplace=True)
df['ACOS_3d'].fillna(float('inf'), inplace=True)
df['ACOS_7d'].fillna(float('inf'), inplace=True)

# 调试信息输出
def print_debug_info(ad_set, step):
    print(f"Step: {step}")
    print(ad_set)
    print("------------\n")

# 定义条件
def filter_good_positions(group):
    result = []
    pos_types = ['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon']

    for pos_type in pos_types:
        subset = group[group['placementClassification'] == pos_type]

        print(f"广告活动: {group['campaignName'].iloc[0]}, 广告位类型: {pos_type}")
        print_debug_info(subset, "过滤前")

        if subset.empty:
            continue

        # 简化条件
        cond = (
            (subset['ACOS_7d'] > 0) & (subset['ACOS_7d'] <= 0.25) & 
            (subset['ACOS_3d'] > 0) & (subset['ACOS_3d'] <= 0.25)
        )

        filtered_subset = subset[cond]
        
        print_debug_info(filtered_subset, "过滤后")

        for idx, row in filtered_subset.iterrows():
            new_bid = min(50, row['bid'] + 5)
            reason = "满足简化条件"
            result.append({
                'campaignName': row['campaignName'],
                'campaignId': row['campaignId'],
                'placementClassification': row['placementClassification'],
                'ACOS_7d': row['ACOS_7d'],
                'ACOS_3d': row['ACOS_3d'],
                'total_clicks_7d': row['total_clicks_7d'],
                'total_clicks_3d': row['total_clicks_3d'],
                'bid': row['bid'],
                'new_bid': new_bid,
                'reason': reason
            })
    
    return result

# 分组处理广告活动
results = df.groupby('campaignId').apply(filter_good_positions)
results = [ad for sublist in results for ad in sublist]

# 生成DataFrame用于保存结果
result_df = pd.DataFrame(results)
print("最终过滤结果:")
print(result_df)

# 保存结果
result_df.to_csv(output_path, index=False)