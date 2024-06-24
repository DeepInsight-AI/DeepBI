# filename: filter_ads.py
import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义原因列初始化为空
data['reason'] = ""

# 定义一：最近7天的总sales为0，但最近7天的总点击数大于0
cond1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)
data.loc[cond1, 'reason'] = "定义一：最近7天的总sales为0，但最近7天的总点击数大于0"

# 定义二：同一广告活动中的三个广告位，最近7天的平均ACOS值都大于0.24小于0.5，且相差大于等于0.2
def meet_condition2(group):
    acoss = group['ACOS_7d']
    if all((acoss > 0.24) & (acoss < 0.5)):
        max_acos = acoss.max()
        min_acos = acoss.min()
        if (max_acos - min_acos) >= 0.2:
            group.loc[acoss.idxmax(), 'reason'] = "定义二：最近7天的平均ACOS值最大的广告位，降低竞价3%"
    return group

data = data.groupby('campaignName').apply(meet_condition2)

# 定义三：最近7天的平均ACOS值大于等于0.5
cond3 = data['ACOS_7d'] >= 0.5
data.loc[cond3, 'reason'] = "定义三：最近7天的平均ACOS值大于等于0.5"

# 筛选出符合条件的广告位
poor_performance_ads = data[data['reason'] != ""]

# 选择需要保存的列
columns_to_save = ['campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'reason']

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_IT_2024-06-11.csv'
poor_performance_ads[columns_to_save].to_csv(output_file_path, index=False)

print("Results saved to", output_file_path)