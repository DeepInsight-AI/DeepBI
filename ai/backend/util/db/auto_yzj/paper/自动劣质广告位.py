# filename: auto_yzj_ad_optimization.py

import pandas as pd

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义函数判断广告位表现
def adjust_bid(data):
    results = []

    campaign_groups = data.groupby('campaignId')

    for campaign_id, group in campaign_groups:
        for _, row in group.iterrows():
            placement = row['placementClassification']
            campaign_name = row['campaignName']
            acos_3d = row['ACOS_3d']
            acos_7d = row['ACOS_7d']
            clicks_7d = row['total_clicks_7d']
            clicks_3d = row['total_clicks_3d']
            sales_7d = row['total_sales14d_7d']
            
            adjustment_reason = ''
            bid_adjustment = ''

            # 处理条件一
            if sales_7d == 0 and clicks_7d > 0:
                bid_adjustment = '变为0'
                adjustment_reason = '最近7天的总sales为0，但最近7天的总点击数大于0'
                results.append([
                    campaign_name, campaign_id, placement, acos_7d, acos_3d, clicks_7d, clicks_3d, 
                    bid_adjustment, adjustment_reason
                ])
                continue

            # 处理条件二
            acos_7d_values = group['ACOS_7d']
            if all(24 <= acos <= 50 for acos in acos_7d_values):
                max_acos_7d = acos_7d_values.max()
                min_acos_7d = acos_7d_values.min()
                if max_acos_7d - min_acos_7d >= 0.2 and acos_7d == max_acos_7d:
                    bid_adjustment = '降低3%'
                    adjustment_reason = '三个广告位中最大和最小的平均ACOS值相差大于等于0.2'
                    results.append([
                        campaign_name, campaign_id, placement, acos_7d, acos_3d, clicks_7d, clicks_3d, 
                        bid_adjustment, adjustment_reason
                    ])
                    continue

            # 处理条件三
            if acos_7d >= 50:
                bid_adjustment = '变为0'
                adjustment_reason = '平均ACOS值大于等于50%'
                results.append([
                    campaign_name, campaign_id, placement, acos_7d, acos_3d, clicks_7d, clicks_3d, 
                    bid_adjustment, adjustment_reason
                ])

    return results

# 调整竞价并输出结果
result_data = adjust_bid(data)
result_df = pd.DataFrame(result_data, columns=[
    'campaignName', 'campaignId', 'placement', '平均ACOS值（最近7天）', '平均ACOS值（最近3天）', 
    '总点击次数（最近7天）', '总点击次数（最近3天）', '竞价调整', '调整原因'
])

output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_IT_2024-06-13.csv'
result_df.to_csv(output_file_path, index=False)

print("竞价调整完成，结果保存在：" + output_file_path)