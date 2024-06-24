# filename: filter_and_output_sku_data.py
import pandas as pd

# 读取原始数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv')

# 定义筛选条件
conditions = [
    (data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24),
    (data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10),
    (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_clicks_7d'] > 13),
    (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24),
    (data['ACOS_7d'] > 0.5),
    (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)
]
choices = [1, 1, 1, 1, 1, 1]
data['selected'] = 0

# 应用条件
for i, condition in enumerate(conditions):
    data.loc[condition, 'selected'] = choices[i]

# 筛选出满足条件的行
selected_data = data[data['selected'] == 1][['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku']]

# 输出结果到CSV文件
selected_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_v1_1_IT_2024-06-13_deepseek.csv', index=False)