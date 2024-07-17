# filename: optimize_ad_slots.py
import pandas as pd

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选条件1: 根据定义一的条件筛选表现较好的广告位
condition1 = (
    (data['placementClassification'].isin(['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon'])) &
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24) &
    (data['total_clicks_7d'] < data.groupby('campaignId')['total_clicks_7d'].transform(max)) &
    (data['total_clicks_3d'] < data.groupby('campaignId')['total_clicks_3d'].transform(max)) &
    (data['ACOS_7d'] == data.groupby('campaignId')['ACOS_7d'].transform(min)) &
    (data['ACOS_3d'] == data.groupby('campaignId')['ACOS_3d'].transform(min))
)

# 筛选条件2: 根据定义二的条件筛选表现较好的广告位
condition2 = (
    (data['placementClassification'].isin(['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon'])) &
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24) &
    (data['total_clicks_7d'] < data.groupby('campaignId')['total_clicks_7d'].transform(max)) &
    (data['total_clicks_3d'] < data.groupby('campaignId')['total_clicks_3d'].transform(max)) &
    (data['total_cost_3d'] < 4) &
    (data['ACOS_7d'] == data.groupby('campaignId')['ACOS_7d'].transform(min))
)

# 将符合条件的广告位筛选出来
filtered_data = data[condition1 | condition2]

# 对符合条件的广告位调整竞价
filtered_data['new_bid'] = filtered_data.apply(lambda row: min(row['bid'] + 5, 50), axis=1)

# 生成原因
filtered_data['reason'] = ''
filtered_data.loc[condition1, 'reason'] = '满足定义一条件'
filtered_data.loc[condition2, 'reason'] = '满足定义二条件'

# 生成输出结果
output_columns = [
    'campaignName', 'campaignId', 'placementClassification',
    'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d',
    'bid', 'new_bid', 'reason'
]
output_data = filtered_data[output_columns]

# 保存到新的csv文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_DE_2024-07-14.csv'
output_data.to_csv(output_file_path, index=False)

print("处理完成，结果已保存至", output_file_path)