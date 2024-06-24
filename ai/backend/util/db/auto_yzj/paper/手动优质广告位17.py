# filename: ad_bid_adjustment.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 计算平均ACOS值和总点击次数
df['avg_ACOS_7d'] = df['ACOS_7d']
df['avg_ACOS_3d'] = df['ACOS_3d']

# 分组计算广告活动内广告位的相对信息
grouped = df.groupby('campaignName')

# 初始化列表以保存符合条件的广告信息
qualified_ads = []

# 遍历每个广告活动
for campaignName, group in grouped:
    # 找到7天和3天内最小的平均ACOS值
    min_avg_acos_7d = group['avg_ACOS_7d'].min()
    min_avg_acos_3d = group['avg_ACOS_3d'].min()
    
    # 计算每个广告位的点击次数
    max_clicks_7d = group['total_clicks_7d'].max()
    max_clicks_3d = group['total_clicks_3d'].max()

    # 遍历每个广告位进行筛选
    for idx, row in group.iterrows():
        if (0 < row['avg_ACOS_7d'] <= 0.24 and row['avg_ACOS_7d'] == min_avg_acos_7d and row['total_clicks_7d'] != max_clicks_7d and
            0 < row['avg_ACOS_3d'] <= 0.24 and row['avg_ACOS_3d'] == min_avg_acos_3d and row['total_clicks_3d'] != max_clicks_3d):
            
            row = row.copy()
            row['调整竞价'] = '提高5%'
            row['原因'] = '满足条件一的广告位'
            qualified_ads.append(row)

# 转换为DataFrame
result_df = pd.DataFrame(qualified_ads)

# 选择需要输出的列
output_columns = [
    'campaignName',
    'campaignId',
    'placementClassification',
    'avg_ACOS_7d',
    'avg_ACOS_3d',
    'total_clicks_7d',
    'total_clicks_3d',
    '调整竞价',
    '原因'
]
result_df = result_df[output_columns]

# 保存结果到指定路径
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_IT_2024-06-17.csv'
result_df.to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")