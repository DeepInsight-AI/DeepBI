# filename: filter_and_adjust_bid.py
import pandas as pd

# 加载数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\预处理.csv"
data = pd.read_csv(file_path)

# 定义竞价提升函数
def adjust_bid(bid, increase_percent, max_increase):
    return min(bid * (1 + increase_percent / 100), bid * (1 + max_increase / 100))

# 筛选符合条件的广告位
def filter_advertising_positions(data):
    data = data[(data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 24) &
                (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 24)]
    
    results = []
    grouped = data.groupby('campaignName')

    for campaign, group in grouped:
        if len(group) < 3:
            continue
          
        group = group.reset_index()
        acos_7d_min = group.loc[group['ACOS_7d'].idxmin()]
        if group['total_clicks_7d'].max() != acos_7d_min['total_clicks_7d']:
            acos_3d_min = group.loc[group['ACOS_3d'].idxmin()]
            if group['total_clicks_3d'].max() != acos_3d_min['total_clicks_3d']:
                bid = adjust_bid(1, 5, 50)  # 暂时假设初始竞价为1，之后根据实际数据调整
                results.append({
                    'campaignName': campaign,
                    'placementClassification': acos_3d_min['placementClassification'],
                    'ACOS_7d': acos_7d_min['ACOS_7d'],
                    'ACOS_3d': acos_3d_min['ACOS_3d'],
                    'total_clicks_7d': acos_7d_min['total_clicks_7d'],
                    'total_clicks_3d': acos_3d_min['total_clicks_3d'],
                    '竞价操作': bid,
                    '原因': '最近7天和3天均为最低ACOS，点击次数不是最大的，且ACOS处于大于0小于等于24%'
                })
                
    return results

filtered_data = filter_advertising_positions(data)
output_df = pd.DataFrame(filtered_data)

# 输出结果保存
output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\优质广告位_FR.csv"
output_df.to_csv(output_file_path, index=False)

print(f"Results have been saved to {output_file_path}")
print(filtered_data if filtered_data else "No matching records found.")