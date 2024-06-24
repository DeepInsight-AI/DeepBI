# filename: adjust_bid_bad_ads.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义输出内容
output = []

# 定义一
def condition_one(row):
    return row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0

# 定义二
def condition_two(group):
    mean_ACOS_7d = group['ACOS_7d'].mean()
    if 24 <= mean_ACOS_7d <= 50:
        max_ACOS_7d = group['ACOS_7d'].max()
        min_ACOS_7d = group['ACOS_7d'].min()
        return max_ACOS_7d - min_ACOS_7d >= 0.2
    return False

# 定义三
def condition_three(row):
    return row['ACOS_7d'] >= 50

# 处理数据
for campaign_id, group in df.groupby('campaignId'):
    for idx, row in group.iterrows():
        reasons = []
        adjust_bid = False
        
        if condition_one(row):
            adjust_bid = True
            reasons.append('最近7天的总sales为0，但最近7天的总点击数大于0')
        
        if condition_two(group):
            max_ACOS_7d_idx = group['ACOS_7d'].idxmax()
            min_ACOS_7d_idx = group['ACOS_7d'].idxmin()
            if idx == max_ACOS_7d_idx:
                adjust_bid = True
                reasons.append('最近7天的平均ACOS值都大于24%小于50%且相差超过0.2，最高者竞价降低3%')
        
        if condition_three(row):
            adjust_bid = True
            reasons.append('最近7天的平均ACOS值大于等于50%的广告位')
        
        if adjust_bid:
            output.append({
                'campaignName': row['campaignName'],
                'campaignId': row['campaignId'],
                'placement': row['placementClassification'],
                'ACOS_7d': row['ACOS_7d'],
                'ACOS_3d': row['ACOS_3d'],
                'clicks_7d': row['total_clicks_7d'],
                'clicks_3d': row['total_clicks_3d'],
                'adjustment': 'set bid to 0' if '大于等于50%' in reasons or '总sales为0' in reasons else 'decrease bid by 3%',
                'reason': '; '.join(reasons)
            })

# 保存结果到CSV文件
output_df = pd.DataFrame(output)
output_filepath = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_ES_2024-06-14.csv'
output_df.to_csv(output_filepath, index=False)
print("结果已保存到:", output_filepath)