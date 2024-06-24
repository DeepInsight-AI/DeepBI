# filename: find_poor_ads.py

import pandas as pd

# 读取数据
input_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv"
output_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_ES_2024-06-10.csv"

# 加载CSV数据
data = pd.read_csv(input_file)

# 定义条件一：最近7天的总sales为0，但最近7天的总点击数大于0的广告位
condition1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)

# 定义条件三：最近7天的平均ACOS值大于等于0.5的广告位
condition3 = data['ACOS_7d'] >= 0.5

# 获取符合条件一和条件三的数据
poor_ads = data[condition1 | condition3].copy()
poor_ads['原因'] = poor_ads.apply(
    lambda row: '原因1' if condition1[row.name] else ('原因3' if condition3[row.name] else ''), axis=1)

# 定义条件二的判断
def check_condition2(df):
    campaign_groups = df.groupby('campaignName')
    res = []

    for campaign, group in campaign_groups:
        if len(group) >= 3:
            group = group.sort_values(by='ACOS_7d', ascending=False)
            if all(0.24 < acos < 0.5 for acos in group['ACOS_7d']):
                acos_diff = group['ACOS_7d'].max() - group['ACOS_7d'].min()
                if acos_diff >= 0.2:
                    ad_to_lower_bid = group.iloc[0].copy()
                    ad_to_lower_bid['原因'] = '原因2'
                    res.append(ad_to_lower_bid)
    
    return res

# 获取符合条件二的数据
condition2_ads = check_condition2(data)
condition2_df = pd.DataFrame(condition2_ads)

# 合并所有符合条件的数据
all_poor_ads = pd.concat([poor_ads, condition2_df], ignore_index=True)

# 只选择相关列
result = all_poor_ads[[
    'campaignName',
    'placementClassification',
    'ACOS_7d',
    'ACOS_3d',
    'total_clicks_7d',
    'total_clicks_3d',
    '原因'
]]

# 保存到CSV
result.to_csv(output_file, index=False)

print("脚本执行完毕，结果已保存到:", output_file)