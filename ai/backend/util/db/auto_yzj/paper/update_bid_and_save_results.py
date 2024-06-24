# filename: update_bid_and_save_results.py

import pandas as pd

# 读入数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 新增列new_bid初始化为bid
df['new_bid'] = df['bid']

# 定义一：最近7天的总sales为0，但最近7天的总点击数大于0的广告位
df.loc[(df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0), 'new_bid'] = 0
df.loc[(df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0), '原因'] = '定义一： 最近7天的总sales为0，但最近7天的总点击数大于0'

# 定义二：对于同一广告活动中，三个广告位中，如果平均ACOS值都大于0.24小于0.5，且ACOS最大与最小相差≥0.2，降ACOS最大的广告位竞价3，直到为0
for campaign_id, sub_df in df.groupby('campaignId'):
    if (sub_df['ACOS_7d'].max() > 0.24) & (sub_df['ACOS_7d'].max() < 0.5) & ((sub_df['ACOS_7d'].max() - sub_df['ACOS_7d'].min()) >= 0.2):
        max_acos_idx = sub_df['ACOS_7d'].idxmax()
        df.loc[max_acos_idx, 'new_bid'] = max(df.loc[max_acos_idx, 'bid'] - 3, 0)
        df.loc[max_acos_idx, '原因'] = '定义二： 对于同一广告活动中，三个广告位中平均ACOS值都大于0.24小于0.5，且ACOS值最大与最小相差≥0.2'

# 定义三：最近7天的平均ACOS值大于等于0.5的广告位
df.loc[(df['ACOS_7d'] >= 0.5), 'new_bid'] = 0
df.loc[(df['ACOS_7d'] >= 0.5), '原因'] = '定义三： 最近7天的平均ACOS值大于等于0.5'

# 选择所需输出的字段
output_columns = [
    'campaignName',
    'campaignId',
    'placementClassification',
    'ACOS_7d',
    'ACOS_3d',
    'total_clicks_7d',
    'total_clicks_3d',
    'bid',
    'new_bid',
    '原因'
]
output_df = df[df['new_bid'] != df['bid']][output_columns]

# 保存结果
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_v1_1_ES_2024-06-20.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Processed data has been saved to {output_file_path}")