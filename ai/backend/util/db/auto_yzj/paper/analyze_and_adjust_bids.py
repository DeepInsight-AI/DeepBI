# filename: analyze_and_adjust_bids.py

import pandas as pd

# Load the CSV file into a DataFrame
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# Identifying Ad Slots satisfying Definition 1 and Definition 2
def identify_good_performance_slots(df):
    result = []

    # Group by campaignId to process each campaign separately
    grouped = df.groupby('campaignId')

    for campaignId, group in grouped:
        for placement in ['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon']:
            subset = group[group['placementClassification'] == placement]

            # Definition 1 criteria
            def1_condition = (
                (subset['ACOS_7d'] > 0.00) & (subset['ACOS_7d'] <= 0.24) &
                (subset['total_clicks_7d'] != subset['total_clicks_7d'].max()) &
                (subset['ACOS_3d'] > 0.00) & (subset['ACOS_3d'] <= 0.24) & 
                (subset['total_clicks_3d'] != subset['total_clicks_3d'].max())
            )

            # Definition 2 criteria
            def2_condition = (
                (subset['ACOS_7d'] > 0.00) & (subset['ACOS_7d'] <= 0.24) &
                (subset['total_clicks_7d'] != subset['total_clicks_7d'].max()) &
                (subset['total_cost_3d'] < 4) &
                (subset['total_clicks_3d'] != subset['total_clicks_3d'].max())
            )

            if def1_condition.any() or def2_condition.any():
                subset_valid = subset[(subset['ACOS_7d'] > 0.00) & (subset['ACOS_7d'] <= 0.24) & (subset['total_clicks_7d'] != subset['total_clicks_7d'].max())]

                for index, row in subset_valid.iterrows():
                    new_bid = min(row['bid'] + 5, 50)
                    reason = "Definition 1" if def1_condition.any() else "Definition 2"
                    result.append({
                        'campaignName': row['campaignName'],
                        'campaignId': row['campaignId'],
                        '广告位': row['placementClassification'],
                        '最近7天的平均ACOS值': row['ACOS_7d'],
                        '最近3天的平均ACOS值': row['ACOS_3d'],
                        '最近7天的总点击次数': row['total_clicks_7d'],
                        '最近3天的总点击次数': row['total_clicks_3d'],
                        'bid(竞价)': row['bid'],
                        '调整后的竞价': new_bid,
                        '调整原因': reason
                    })

    return result

# Identify good performance ad slots
good_performance_slots = identify_good_performance_slots(df)

# Convert the result to DataFrame and save to CSV
output_df = pd.DataFrame(good_performance_slots)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_DE_2024-07-12.csv'
output_df.to_csv(output_file_path, index=False)

print("Analysis complete. The results are saved to:", output_file_path)