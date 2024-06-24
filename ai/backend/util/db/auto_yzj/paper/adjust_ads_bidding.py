# filename: adjust_ads_bidding.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 初始化竞价调整和原因列
data['竞价调整'] = ''
data['原因'] = ''

# 筛选和处理数据
bad_ads = []

# 定义一：最近7天的总sales为0，但最近7天的总点击数大于0
condition1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)
bad_ads_def1 = data[condition1].copy()
bad_ads_def1['竞价调整'] = '变为0'
bad_ads_def1['原因'] = '定义一: 最近7天总sales为0且总点击数大于0'

bad_ads.append(bad_ads_def1)

# 定义二：同一广告活动中3个广告位平均ACOS值均大于0.24小于0.5，最大值和最小值相差 >= 0.2
campaign_groups = data.groupby('campaignName')
for name, group in campaign_groups:
    if len(group) == 3:
        avg_acos_7d = group['ACOS_7d']
        if avg_acos_7d.between(0.24, 0.5).all():
            max_acos = avg_acos_7d.max()
            min_acos = avg_acos_7d.min()
            if max_acos - min_acos >= 0.2:
                max_acos_index = group['ACOS_7d'].idxmax()
                data.loc[max_acos_index, '竞价调整'] = '降低3%'
                data.loc[max_acos_index, '原因'] = '定义二: 最近7天平均ACOS值相差0.2及以上，且都是大于0.24小于0.5'

# 筛选出符合定义二条件的广告位
bad_ads_def2 = data[data['竞价调整'] == '降低3%']
bad_ads.append(bad_ads_def2)

# 定义三：最近7天平均ACOS值 >= 0.5
condition3 = data['ACOS_7d'] >= 0.5
bad_ads_def3 = data[condition3].copy()
bad_ads_def3['竞价调整'] = '变为0'
bad_ads_def3['原因'] = '定义三: 最近7天平均ACOS值大于等于0.5'

bad_ads.append(bad_ads_def3)

# 合并所有不符合条件的广告位
final_bad_ads = pd.concat(bad_ads)

# 选择需要的列保存到CSV
columns_to_save = [
    'campaignName',
    'placementClassification',
    'ACOS_7d',
    'ACOS_3d',
    'total_clicks_7d',
    'total_clicks_3d',
    '竞价调整',
    '原因'
]
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\劣质广告位_FR.csv'
final_bad_ads.to_csv(output_file_path, columns=columns_to_save, index=False)

print(f"结果已成功保存到 {output_file_path}")