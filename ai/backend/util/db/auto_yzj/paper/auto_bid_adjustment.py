# filename: auto_bid_adjustment.py
import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选条件
def filter_ads(data):
    # 条件1: 广告位最近7天的平均ACOS值最小，大于0且小于等于0.24，但最近7天点击次数不是最大
    condition1 = data.groupby('campaignName').apply(lambda x: x[(x['ACOS_7d'] == x['ACOS_7d'][x['ACOS_7d'] > 0].min()) & 
                                                               (x['ACOS_7d'] <= 0.24) & 
                                                               (x['total_clicks_7d'] != x['total_clicks_7d'].max())]).reset_index(drop=True)

    # 条件2: 广告位最近3天的平均ACOS值最小，大于0且小于等于0.24，但最近3天点击次数不是最大
    condition2 = data.groupby('campaignName').apply(lambda x: x[(x['ACOS_3d'] == x['ACOS_3d'][x['ACOS_3d'] > 0].min()) & 
                                                               (x['ACOS_3d'] <= 0.24) & 
                                                               (x['total_clicks_3d'] != x['total_clicks_3d'].max())]).reset_index(drop=True)

    # 同时满足条件1和条件2
    filtered_ads = pd.merge(condition1, condition2, on=['campaignName', 'placementClassification'], suffixes=('_7d', '_3d'))
    
    # 重命名相关的列
    filtered_ads.rename(columns={
        "ACOS_7d_7d": "ACOS_7d",
        "ACOS_3d_3d": "ACOS_3d",
        "total_clicks_7d_7d": "total_clicks_7d",
        "total_clicks_3d_3d": "total_clicks_3d"
    }, inplace=True)
    
    return filtered_ads

# 筛选符合条件的广告位
filtered_ads = filter_ads(data)

# 添加竞价操作原因
filtered_ads['原因'] = '最近7天和3天的平均ACOS值最小，且点击次数不是最大，提高竞价5%，最高50%'

# 选择所需的列
filtered_ads = filtered_ads[['campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', '原因']]

# 保存结果到新CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_IT_2024-06-11.csv'
filtered_ads.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存至 {output_file_path}")