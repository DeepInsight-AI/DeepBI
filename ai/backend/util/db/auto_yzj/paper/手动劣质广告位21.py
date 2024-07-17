# filename: ad_placement_optimization.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义一个函数以0竞价的广告位

def process_bid_zero(df):
    df['new_bid'] = df['bid']
    df.loc[(df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0), 'new_bid'] = 0
    return df

# 定义一个函数来根据条件2修改竞价
def process_bid_reduction(df):
    grouped = df.groupby('campaignId')
    for name, group in grouped:
        if len(group) == 3:
            acos_mean_7d = group['ACOS_7d']
            if acos_mean_7d.between(0.24, 0.5).all():
                if acos_mean_7d.max() - acos_mean_7d.min() >= 0.2:
                    highest_acos_index = acos_mean_7d.idxmax()
                    df.loc[highest_acos_index, 'new_bid'] = max(0, df.loc[highest_acos_index, 'bid'] - 3)
    return df

# 定义一个函数以0竞价的广告位在条件3下
def process_bid_zero_condition_three(df):
    grouped = df.groupby('campaignId')
    for name, group in grouped:
        if len(group) == 3:
            acos_mean_7d = group['ACOS_7d']
            if acos_mean_7d.min() >= 0.5:
                df.loc[group.index, 'new_bid'] = 0
    return df

# 添加 'new_bid' 列
data['new_bid'] = data['bid']

# 按照定义1处理
data = process_bid_zero(data)

# 按照定义2处理
data = process_bid_reduction(data)

# 按照定义3处理
data = process_bid_zero_condition_three(data)

# 添加对广告位进行竞价操作的具体原因
data['reason'] = ''

# 设置原因
data.loc[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0), 'reason'] = '最近7天的总sales为0，但最近7天的总点击数大于0'
grouped = data.groupby('campaignId')
for name, group in grouped:
    if len(group) == 3:
        acos_mean_7d = group['ACOS_7d']
        if acos_mean_7d.between(0.24, 0.5).all() and acos_mean_7d.max() - acos_mean_7d.min() >= 0.2:
            highest_acos_index = acos_mean_7d.idxmax()
            data.loc[highest_acos_index, 'reason'] = '最近7天的平均ACOS值在0.24到0.5之间，并且ACOS值最大的广告位与最小的差值大于等于0.2'
        if acos_mean_7d.min() >= 0.5:
            data.loc[group.index, 'reason'] = '最近7天的平均ACOS值大于等于0.5'

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_v1_1_IT_2024-06-211.csv'
data.to_csv(output_file_path, index=False)

print(f'Results saved to {output_file_path}')