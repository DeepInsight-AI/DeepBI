# filename: process_ad_placements.py
import pandas as pd
import numpy as np

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义条件一，二，三
def condition_1(row):
    return row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0

def condition_2(rows):
    acoss = rows['ACOS_7d']
    min_acos = acoss.min()
    max_acos = acoss.max()
    return (all((24 < x < 50) for x in acoss) and (max_acos - min_acos) >= 0.2)

def condition_3(row):
    return row['ACOS_7d'] >= 50

# 新增列来存储新的竞价和原因
df['new_bid'] = df['bid']
df['reason'] = np.nan

# 遍历数据框，进行判断和操作
for i, row in df.iterrows():
    if condition_1(row):
        df.at[i, 'new_bid'] = 0
        df.at[i, 'reason'] = '定义一: 最近7天的总sales为0，但最近7天的总点击数大于0'
    elif condition_3(row):
        df.at[i, 'new_bid'] = 0
        df.at[i, 'reason'] = '定义三: 最近7天的平均ACOS值大于等于50%'
        
# 对于同一广告活动中广告位进行定义二的判断
campaign_groups = df.groupby('campaignId')
for campaign_id, group in campaign_groups:
    if len(group) >= 3 and condition_2(group):
        max_acos_index = group['ACOS_7d'].idxmax()
        df.at[max_acos_index, 'new_bid'] *= 0.97
        if df.at[max_acos_index, 'new_bid'] < 0:
            df.at[max_acos_index, 'new_bid'] = 0
        df.at[max_acos_index, 'reason'] = (
            '定义二: 同一广告活动中位置平均ACOS值都大于24%小于50%, '
            '并且最大ACOS值与最小ACOS值相差>= 0.2')

# 过滤出需要的列进行输出
output_df = df[['campaignName', 'campaignId', 'placementClassification',
                'ACOS_7d', 'total_clicks_7d', 'total_sales14d_7d', 
                'total_clicks_3d', 'bid', 'new_bid', 'reason']]

# 保存到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_IT_2024-06-19.csv'
output_df.to_csv(output_path, index=False)

print("Processing complete. Results saved to:", output_path)