# filename: analyze_ad_placement.py
import pandas as pd

# 读取数据集
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(data_path)

# 定义一的判断：最近7天的总Sales为0，但最近7天的总点击数大于0的广告位
df['new_bid'] = df['bid']  # 初始化new_bid
definition1 = (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0)
df.loc[definition1, 'new_bid'] = 0
df.loc[definition1, 'reason'] = 'Definition 1: Last 7 days total sales = 0 but clicks > 0'

# 定义二的判断：
def check_definition_2(group):
    if len(group) == 3 and all((group['ACOS_7d'] > 24) & (group['ACOS_7d'] < 50)):
        max_acos = group['ACOS_7d'].max()
        min_acos = group['ACOS_7d'].min()
        if max_acos - min_acos >= 0.2:
            max_acos_idx = group['ACOS_7d'].idxmax()
            group.loc[max_acos_idx, 'new_bid'] = group.loc[max_acos_idx, 'bid'] * 0.97
            group.loc[max_acos_idx, 'reason'] = 'Definition 2: ACOS difference >= 0.2 and all in [24%, 50%)'
    return group

df = df.groupby('campaignId').apply(check_definition_2)

# 定义三的判断：最近7天的平均ACOS值大于等于50%的广告位
definition3 = (df['ACOS_7d'] >= 50)
df.loc[definition3, 'new_bid'] = 0
df.loc[definition3, 'reason'] = 'Definition 3: Last 7 days average ACOS >= 50%'

# 过滤包含调整竞价的记录，并输出到CSV文件
output_columns = [
    'campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 
    'total_clicks_7d', 'total_clicks_3d', 'bid', 'new_bid', 'reason'
]
result_df = df[df['reason'].notna()][output_columns]

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_ES_2024-06-18.csv'
result_df.to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")