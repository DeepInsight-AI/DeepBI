# filename: analyze_poor_performance.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义一个新的bid初始化为原bid
data['new_bid'] = data['bid']
data['原因'] = ''

# 满足定义一
condition1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)
data.loc[condition1, 'new_bid'] = 0
data.loc[condition1, '原因'] = '最近7天的总sales为0，但最近7天的总点击数大于0'

# 满足定义三
condition3 = data['ACOS_7d'] >= 50
data.loc[condition3, 'new_bid'] = 0
data.loc[condition3, '原因'] = '最近7天的平均ACOS值大于等于50%'

# 满足定义二
campaigns = data['campaignName'].unique()
for campaign in campaigns:
    campaign_data = data[data['campaignName'] == campaign]
    if len(campaign_data['placementClassification'].unique()) >= 3:
        acos_values = campaign_data['ACOS_7d']
        if acos_values.between(24, 50).all() and (acos_values.max() - acos_values.min() >= 0.2):
            max_acos_index = campaign_data['ACOS_7d'].idxmax()
            new_bid_value = data.loc[max_acos_index, 'bid'] - 3
            data.at[max_acos_index, 'new_bid'] = max(0, new_bid_value)
            data.at[max_acos_index, '原因'] = '同一广告活动中，最近7天的平均ACOS值在24%至50%之间，且差值大于等于0.2'

# 选择需要的列
result = data[['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'new_bid', '原因']]

# 保存结果到CSV文件
result_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_IT_2024-06-211.csv'
result.to_csv(result_file_path, index=False)

print("CSV文件已成功生成。")