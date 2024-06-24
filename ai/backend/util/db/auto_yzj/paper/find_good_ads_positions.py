# filename: find_good_ads_positions.py
import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 计算最近7天和最近3天的平均ACOS值
data['avg_ACOS_7d'] = data.groupby('campaignId')['ACOS_7d'].transform('mean')
data['avg_ACOS_3d'] = data.groupby('campaignId')['ACOS_3d'].transform('mean')

# 筛选表现较好的广告位
good_ads_positions = data[
    (data['avg_ACOS_7d'] == data.groupby('campaignId')['avg_ACOS_7d'].transform('min')) &
    (data['avg_ACOS_7d'] > 0) & 
    (data['avg_ACOS_7d'] <= 0.24) & 
    (data['total_clicks_7d'] != data.groupby('campaignId')['total_clicks_7d'].transform('max')) &
    (data['avg_ACOS_3d'] == data.groupby('campaignId')['avg_ACOS_3d'].transform('min')) &
    (data['avg_ACOS_3d'] > 0) &
    (data['avg_ACOS_3d'] <= 0.24) &
    (data['total_clicks_3d'] != data.groupby('campaignId')['total_clicks_3d'].transform('max'))
]

# 对符合条件的广告位调整竞价
good_ads_positions['bid_adjustment'] = '提高竞价5%，直到50%'
good_ads_positions['reason'] = '最近7天和最近3天的平均ACOS值最小且在0到0.24之间，但点击次数不是最大'

# 生成输出的CSV
output_columns = [
    'campaignName', 
    'campaignId', 
    'placementClassification',
    'avg_ACOS_7d', 
    'avg_ACOS_3d', 
    'total_clicks_7d', 
    'total_clicks_3d',
    'bid_adjustment',
    'reason'
]

output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_ES_2024-06-121.csv'
good_ads_positions.to_csv(output_file_path, columns=output_columns, index=False)

print('CSV file has been generated and saved to:', output_file_path)