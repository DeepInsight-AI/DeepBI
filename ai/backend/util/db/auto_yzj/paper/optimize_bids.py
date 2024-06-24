# filename: optimize_bids.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 计算各项指标
data['ACOS_3d'] = data['total_cost_3d'] / data['total_sales14d_3d']
data['ACOS_7d'] = data['total_cost_7d'] / data['total_sales14d_7d']

# 找出表现较好的广告位
conditions = (
    (data['ACOS_7d'] <= 0.24) & (data['ACOS_7d'] > 0) &
    (data.groupby('campaignId')['total_clicks_7d'].transform('max') != data['total_clicks_7d']) & 
    (data.groupby('campaignId')['ACOS_7d'].transform('min') == data['ACOS_7d']) &
    (data['ACOS_3d'] <= 0.24) & (data['ACOS_3d'] > 0) & 
    (data.groupby('campaignId')['total_clicks_3d'].transform('max') != data['total_clicks_3d']) & 
    (data.groupby('campaignId')['ACOS_3d'].transform('min') == data['ACOS_3d'])
)

good_placements = data[conditions]

# 调整竞价
good_placements['bid_adjustment'] = 5  # 假设初始竞价调整为5%
good_placements['reason'] = '最小ACOS广告位，调整竞价5％，直至到达50％'

# 输出结果
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_ES_2024-06-12.csv'
columns_to_save = [
    'campaignName', 'campaignId', 'placementClassification', 
    'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 
    'bid_adjustment', 'reason'
]

good_placements.to_csv(output_file_path, columns=columns_to_save, index=False)
print("结果已保存到：", output_file_path)