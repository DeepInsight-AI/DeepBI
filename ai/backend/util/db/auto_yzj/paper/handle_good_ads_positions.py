# filename: handle_good_ads_positions.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选数据，选择表现较好的广告位并更新竞价
def filter_and_update_bids(df):
    reasons = []

    filtered_df = pd.DataFrame(columns=df.columns)

    for campaign in df['campaignId'].unique():
        campaign_df = df[df['campaignId'] == campaign]

        for placement in ["Top of Search on-Amazon", "Detail Page on-Amazon", "Other on-Amazon"]:
            placement_df = campaign_df[campaign_df['placementClassification'] == placement]

            if len(placement_df) == 0:
                continue

            # 定义一：检查最近7天和3天的ACOS以及点击次数
            condition1 = (
                (placement_df['ACOS_7d'].min() > 0) &
                (placement_df['ACOS_7d'].min() <= 0.24) &
                (placement_df['total_clicks_7d'] != placement_df['total_clicks_7d'].max()) &
                (placement_df['ACOS_3d'].min() > 0) &
                (placement_df['ACOS_3d'].min() <= 0.24) &
                (placement_df['total_clicks_3d'] != placement_df['total_clicks_3d'].max())
            )

            # 定义二：检查最近7天的ACOS和点击次数，总花费
            condition2 = (
                (placement_df['ACOS_7d'].min() > 0) &
                (placement_df['ACOS_7d'].min() <= 0.24) &
                (placement_df['total_clicks_7d'] != placement_df['total_clicks_7d'].max()) &
                (placement_df['total_cost_3d'] < 4) &
                (placement_df['total_clicks_3d'] != placement_df['total_clicks_3d'].max())
            )

            if condition1.any():
                reason = "满足定义一条件"
                reasons.append(reason)
                selected_row = placement_df.loc[condition1.idxmax()]
                updated_bid = min(selected_row['bid'] + 5, 50)
                selected_row['updated_bid'] = updated_bid
                filtered_df = pd.concat([filtered_df, selected_row.to_frame().T])
            elif condition2.any():
                reason = "满足定义二条件"
                reasons.append(reason)
                selected_row = placement_df.loc[condition2.idxmax()]
                updated_bid = min(selected_row['bid'] + 5, 50)
                selected_row['updated_bid'] = updated_bid
                filtered_df = pd.concat([filtered_df, selected_row.to_frame().T])

    return filtered_df, reasons

# 执行筛选和竞价更新
filtered_df, reasons = filter_and_update_bids(df)

# 添加原因列
filtered_df['reason'] = reasons

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_US_2024-07-14.csv'
filtered_df.to_csv(output_path, index=False)

print("处理完成，结果已保存到目标文件。")