# filename: filter_and_adjust_bids.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv')

# 定义一：最近7天的总sales为0，但最近7天的总点击数大于0的广告位
def_one_mask = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)

# 定义二：同一广告活动中的三个广告位，最近7天的平均ACOS值都大于0.24小于0.5，且最大和最小ACOS值相差大于等于0.2
def_two_mask = data.groupby('campaignName')['ACOS_7d'].transform(lambda x: (x > 0.24) & (x < 0.5) & ((x.max() - x.min()) >= 0.2))

# 定义三：最近7天的平均ACOS值大于等于0.5的广告位
def_three_mask = data['ACOS_7d'] >= 0.5

# 合并所有条件
mask = def_one_mask | def_two_mask | def_three_mask

# 应用筛选条件
filtered_data = data[mask]

# 竞价调整
filtered_data['竞价调整'] = 0
filtered_data.loc[def_one_mask, '竞价调整'] = 0
filtered_data.loc[def_three_mask, '竞价调整'] = 0
filtered_data.loc[def_two_mask, '竞价调整'] = filtered_data.loc[def_two_mask, '竞价调整'] * 0.97

# 添加原因列
filtered_data['对广告位进行竞价操作的具体原因'] = ''
filtered_data.loc[def_one_mask, '对广告位进行竞价操作的具体原因'] = '定义一：最近7天的总sales为0，但最近7天的总点击数大于0'
filtered_data.loc[def_three_mask, '对广告位进行竞价操作的具体原因'] = '定义三：最近7天的平均ACOS值大于等于0.5'
filtered_data.loc[def_two_mask, '对广告位进行竞价操作的具体原因'] = '定义二：同一广告活动中的三个广告位，最近7天的平均ACOS值都大于0.24小于0.5，且最大和最小ACOS值相差大于等于0.2'

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\劣质广告位_FR_2024-5-27_deepseek.csv'
filtered_data.to_csv(output_file_path, index=False)

print("结果已保存到CSV文件。")