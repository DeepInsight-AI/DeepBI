# filename: filter_and_save_data.py
import pandas as pd

# 读取原始数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv')

# 定义筛选条件
conditions = [
    (data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24),
    (data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10),
    (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_clicks_7d'] > 13),
    (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24),
    (data['ACOS_7d'] > 0.5),
    (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)
]
choices = ['定义一', '定义二', '定义三', '定义四', '定义五', '定义六']
data['关闭原因'] = '无'

# 应用条件并标记关闭原因
for i, condition in enumerate(conditions):
    data.loc[condition, '关闭原因'] = choices[i]

# 筛选出需要关闭的SKU数据
filtered_data = data[data['关闭原因'] != '无']

# 保存筛选后的数据到新的CSV文件
filtered_data[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭原因']].to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\关闭SKU_FR_2024-5-27_deepseek.csv', index=False)