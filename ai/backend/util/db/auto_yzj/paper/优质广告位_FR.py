# filename: 优质广告位_FR.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\预处理.csv"
df = pd.read_csv(file_path)

# 定义竞价操作以及其原因
def bid_adjustment(row):
    return "提高竞价5%", "满足条件1和条件2"

# 初始化一个空列表用于存储符合条件的广告位信息
result_columns = [
    "campaignName",
    "placementClassification",
    "ACOS_7d",
    "ACOS_3d",
    "total_clicks_7d",
    "total_clicks_3d",
    "竞价操作",
    "原因"
]
result_list = []

# 对每个广告活动进行处理
for campaign in df['campaignName'].unique():
    campaign_data = df[df['campaignName'] == campaign]
    
    # 筛选符合条件1和条件2的广告位
    condition1_idx = (campaign_data['ACOS_7d'] > 0) & (campaign_data['ACOS_7d'] <= 24)
    condition2_idx = (campaign_data['ACOS_3d'] > 0) & (campaign_data['ACOS_3d'] <= 24)
    
    if condition1_idx.sum() > 0 and condition2_idx.sum() > 0:
        best_ACOS_7d = campaign_data.loc[condition1_idx, 'ACOS_7d'].min()
        best_ACOS_3d = campaign_data.loc[condition2_idx, 'ACOS_3d'].min()
        
        condition1 = (campaign_data['ACOS_7d'] == best_ACOS_7d) & (campaign_data['total_clicks_7d'] != campaign_data['total_clicks_7d'].max())
        condition2 = (campaign_data['ACOS_3d'] == best_ACOS_3d) & (campaign_data['total_clicks_3d'] != campaign_data['total_clicks_3d'].max())
        
        qualified_ads = campaign_data.loc[condition1 & condition2]
        
        for _, row in qualified_ads.iterrows():
            # 获取竞价操作及原因
            bid_adjustment_result = bid_adjustment(row)
            result_list.append([
                row['campaignName'],
                row['placementClassification'],
                row['ACOS_7d'],
                row['ACOS_3d'],
                row['total_clicks_7d'],
                row['total_clicks_3d'],
                bid_adjustment_result[0],
                bid_adjustment_result[1]
            ])

# 将结果保存到新的CSV文件中
output_df = pd.DataFrame(result_list, columns=result_columns)
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\优质广告位_FR.csv"
output_df.to_csv(output_path, index=False)

print(f"结果已保存到: {output_path}")