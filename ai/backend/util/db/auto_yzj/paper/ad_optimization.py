# filename: ad_optimization.py
import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 昨天日期
yesterday_date = '2024-05-26'

# 筛选表现较差的广告位
poor_performance_ads = []

# 定义一：最近7天的总sales为0，但最近7天的总点击数大于0的广告位
condition1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)
poor_performance_ads.extend(data[condition1].assign(定义='定义一').to_dict('records'))

# 定义二：同一广告活动中三个广告位的ACOS条件
campaigns = data.groupby('campaignName')
for campaign_name, group in campaigns:
    if len(group) >= 3:
        acos_7d_values = group['ACOS_7d']
        if acos_7d_values.between(24, 50).all():
            max_acos = acos_7d_values.max()
            min_acos = acos_7d_values.min()
            if max_acos - min_acos >= 0.2:
                max_acos_ad = group[group['ACOS_7d'] == max_acos]
                poor_performance_ads.extend(max_acos_ad.assign(定义='定义二').to_dict('records'))

# 定义三：最近7天的平均ACOS值大于等于50%的广告位
condition3 = data['ACOS_7d'] >= 50
poor_performance_ads.extend(data[condition3].assign(定义='定义三').to_dict('records'))

# 准备输出结果
output_dataframe = pd.DataFrame(poor_performance_ads)
output_columns = [
    'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d',
    'total_clicks_7d', 'total_clicks_3d', '定义'
]
output_dataframe.to_csv(
    r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_ES_2024-06-07.csv',
    columns=output_columns,
    index=False,
    encoding='utf-8-sig'
)

print("结果已经保存到指定文件。")