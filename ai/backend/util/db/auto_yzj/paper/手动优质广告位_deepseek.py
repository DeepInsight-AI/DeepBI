# filename: save_results.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv')

# 确保ACOS和点击次数的列不为NaN
data['ACOS_7d'] = data['ACOS_7d'].fillna(0)
data['ACOS_3d'] = data['ACOS_3d'].fillna(0)
data['total_clicks_7d'] = data['total_clicks_7d'].fillna(0)
data['total_clicks_3d'] = data['total_clicks_3d'].fillna(0)

# 筛选满足条件的广告位
filtered_data = data[
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24) &
    (data['total_clicks_7d'] > 0) & (data['total_clicks_3d'] > 0)
]

# 找出每个campaignName下ACOS最小的广告位，且点击次数不是最大
best_ads = []
for name, group in filtered_data.groupby('campaignName'):
    min_acos_7d = group[group['ACOS_7d'] == group['ACOS_7d'].min()]
    if not min_acos_7d.empty:
        if min_acos_7d['total_clicks_7d'].max() != group['total_clicks_7d'].max():
            best_ads.append(min_acos_7d)

    min_acos_3d = group[group['ACOS_3d'] == group['ACOS_3d'].min()]
    if not min_acos_3d.empty:
        if min_acos_3d['total_clicks_3d'].max() != group['total_clicks_3d'].max():
            best_ads.append(min_acos_3d)

# 合并结果
best_ads = pd.concat(best_ads)

# 添加竞价调整和原因列
best_ads['竞价调整'] = '提高5%-50%'
best_ads['对广告位进行竞价操作的原因'] = '满足定义一的条件：最近7天和最近3天的平均ACOS值最小，且ACOS值大于0小于等于0.24，同时点击次数不是最大。'

# 选择需要的列
output_data = best_ads[['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', '竞价调整', '对广告位进行竞价操作的原因']]

# 保存到CSV文件
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_IT_2024-06-13_deepseek.csv', index=False)

# 打印输出数据
print(output_data)