# filename: identify_poor_performing_ads.py
import pandas as pd

# 读取之前处理过的数据集
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv')

# 确保数据集中包含所有必要的字段
required_columns = [
    "campaignName", "campaignId", "placementClassification",
    "total_clicks_3d", "total_clicks_7d", "total_sales14d_7d",
    "total_cost_3d", "total_cost_7d", "ACOS_3d", "ACOS_7d"
]
missing_columns = [col for col in required_columns if col not in data.columns]

if missing_columns:
    print(f"Missing columns: {missing_columns}")
else:
    print("All required columns are present.")

# 计算最近7天的平均ACOS值
data['average_ACOS_7d'] = data.apply(lambda row: row['total_cost_7d'] / row['total_sales14d_7d'] if row['total_sales14d_7d'] > 0 else 0, axis=1)

# 输出数据集的前几行以检查数据
print(data.head())

# 定义一：最近7天的总sales为0，但最近7天的总点击数大于0的广告位
def condition_one(row):
    return row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0

# 定义二：同一广告活动中，三个广告位中，最近7天的平均ACOS值最大的广告位与最小的广告位ACOS值相差大于等于0.2
def condition_two(group):
    if len(group) >= 3:
        acos_values = group['average_ACOS_7d']
        max_acos = acos_values.max()
        min_acos = acos_values.min()
        result = max_acos - min_acos >= 0.2 and (max_acos > 0.24 and max_acos < 0.5)
        return pd.Series(result, index=['condition_two'])
    return pd.Series(False, index=['condition_two'])

# 定义三：同一广告活动中，三个广告位中，最近7天的平均ACOS值大于等于0.5的广告位
def condition_three(row):
    return row['average_ACOS_7d'] >= 0.5

# 应用条件
data['condition_one'] = data.apply(condition_one, axis=1)
data['condition_two'] = data.groupby(['campaignName', 'campaignId']).apply(condition_two)
data['condition_three'] = data.apply(condition_three, axis=1)

# 确定竞价调整
data['bid_adjustment'] = 0
data.loc[data['condition_one'], 'bid_adjustment'] = 0
data.loc[data['condition_two'], 'bid_adjustment'] = -0.03
data.loc[data['condition_three'], 'bid_adjustment'] = 0

# 输出结果
output_data = data[['campaignName', 'campaignId', 'placementClassification', 'average_ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid_adjustment']]
output_data.columns = [
    'campaignName', 'campaignId', '广告位', '最近7天的平均ACOS值', '最近3天的平均ACOS值',
    '最近7天的总点击次数', '最近3天的总点击次数', '竞价调整'
]
output_data['对广告位进行竞价操作的具体原因'] = ''
output_data.loc[output_data['竞价调整'] == 0, '对广告位进行竞价操作的具体原因'] = '定义一：最近7天的总sales为0，但最近7天的总点击数大于0'
output_data.loc[output_data['竞价调整'] == -0.03, '对广告位进行竞价操作的具体原因'] = '定义二：最近7天的平均ACOS值最大的广告位与最小的广告位ACOS值相差大于等于0.2'
output_data.loc[output_data['竞价调整'] == 0, '对广告位进行竞价操作的具体原因'] = '定义三：最近7天的平均ACOS值大于等于0.5'

# 保存到CSV文件
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_v1_1_IT_2024-06-13_deepseek.csv', index=False)