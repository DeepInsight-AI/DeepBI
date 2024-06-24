# filename: adjust_bidding_strategy.py
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv')

# 定义函数来筛选和调整竞价
def adjust_bidding(df):
    # 初始化结果列表
    results = []
    
    # 遍历数据
    for campaign_id in df['campaignId'].unique():
        campaign_data = df[df['campaignId'] == campaign_id]
        
        # 检查定义一
        zero_sales_high_clicks = campaign_data[(campaign_data['total_sales14d_7d'] == 0) & (campaign_data['total_clicks_7d'] > 0)]
        if not zero_sales_high_clicks.empty:
            for _, row in zero_sales_high_clicks.iterrows():
                results.append({
                    'campaignName': row['campaignName'],
                    'campaignId': row['campaignId'],
                    'placementClassification': row['placementClassification'],
                    'ACOS_7d': row['ACOS_7d'],
                    'ACOS_3d': row['ACOS_3d'],
                    'total_clicks_7d': row['total_clicks_7d'],
                    'total_clicks_3d': row['total_clicks_3d'],
                    '竞价调整': 0,
                    '对广告位进行竞价操作的具体原因': '定义一'
                })
        
        # 检查定义二和定义三
        if len(campaign_data) >= 3:
            avg_acos_7d = campaign_data['ACOS_7d'].mean()
            max_acos_7d = campaign_data['ACOS_7d'].max()
            min_acos_7d = campaign_data['ACOS_7d'].min()
            
            if avg_acos_7d > 24 and avg_acos_7d < 50 and (max_acos_7d - min_acos_7d) >= 0.2:
                high_acos_ad = campaign_data[campaign_data['ACOS_7d'] == max_acos_7d]
                for _, row in high_acos_ad.iterrows():
                    results.append({
                        'campaignName': row['campaignName'],
                        'campaignId': row['campaignId'],
                        'placementClassification': row['placementClassification'],
                        'ACOS_7d': row['ACOS_7d'],
                        'ACOS_3d': row['ACOS_3d'],
                        'total_clicks_7d': row['total_clicks_7d'],
                        'total_clicks_3d': row['total_clicks_3d'],
                        '竞价调整': -0.03,
                        '对广告位进行竞价操作的具体原因': '定义二'
                    })
            
            if avg_acos_7d >= 50:
                high_acos_ad = campaign_data[campaign_data['ACOS_7d'] >= 50]
                for _, row in high_acos_ad.iterrows():
                    results.append({
                        'campaignName': row['campaignName'],
                        'campaignId': row['campaignId'],
                        'placementClassification': row['placementClassification'],
                        'ACOS_7d': row['ACOS_7d'],
                        'ACOS_3d': row['ACOS_3d'],
                        'total_clicks_7d': row['total_clicks_7d'],
                        'total_clicks_3d': row['total_clicks_3d'],
                        '竞价调整': 0,
                        '对广告位进行竞价操作的具体原因': '定义三'
                    })
    
    # 创建结果DataFrame
    result_df = pd.DataFrame(results)
    
    # 输出结果到CSV文件
    result_df.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_IT_2024-06-13_deepseek.csv', index=False)

# 执行竞价调整
adjust_bidding(data)