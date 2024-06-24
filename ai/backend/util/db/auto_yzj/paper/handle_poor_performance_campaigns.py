# filename: handle_poor_performance_campaigns.py

import pandas as pd

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义竞价操作函数，模拟操作
def change_bid(campaign_name, placement_name, action):
    return {
        'campaignName': campaign_name,
        'placementClassification': placement_name,
        'action': action
    }

# 筛选符合定义一的广告位
def1 = data[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)]
def1_actions = [change_bid(row['campaignName'], row['placementClassification'], 'set bid to 0')
                for _, row in def1.iterrows()]

# 筛选符合定义二的广告位
def2_actions = []
grouped_campaigns = data.groupby('campaignName')
for _, group in grouped_campaigns:
    if len(group) >= 3:
        acoss = group['ACOS_7d']
        if all(acoss > 24) and all(acoss < 50):
            max_acos = acoss.max()
            min_acos = acoss.min()
            if max_acos - min_acos >= 0.2:
                max_acos_row = group[group['ACOS_7d'] == max_acos].iloc[0]
                def2_actions.append(change_bid(max_acos_row['campaignName'], max_acos_row['placementClassification'], 'lower bid by 3%'))

# 筛选符合定义三的广告位
def3 = data[data['ACOS_7d'] >= 50]
def3_actions = [change_bid(row['campaignName'], row['placementClassification'], 'set bid to 0')
                for _, row in def3.iterrows()]

# 合并所有操作
all_actions = def1_actions + def2_actions + def3_actions

# 将结果保存到CSV文件
result_df = pd.DataFrame(all_actions)
result_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_IT_2024-06-08.csv'
result_df.to_csv(result_path, index=False, encoding='utf-8')

print("结果已保存到:", result_path)