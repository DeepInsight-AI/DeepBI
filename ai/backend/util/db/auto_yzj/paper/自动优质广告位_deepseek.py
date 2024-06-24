# filename: filter_and_adjust.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv')

# 筛选条件
conditions = (
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24) &
    (data['total_clicks_7d'] != data['total_clicks_7d'].max()) &
    (data['total_clicks_3d'] != data['total_clicks_3d'].max())
)

# 筛选数据
filtered_data = data[conditions]

# 计算竞价调整
max_adjustment = 0.50  # 50%
adjustment_step = 0.05  # 5%

# 初始化竞价调整列
filtered_data['竞价调整'] = 0

# 计算竞价调整
for index, row in filtered_data.iterrows():
    # 假设竞价调整从5%开始，每次增加5%，直到达到50%
    for adjustment in range(1, int(max_adjustment / adjustment_step) + 1):
        filtered_data.at[index, '竞价调整'] = adjustment * adjustment_step
        break  # 只进行一次调整

# 添加原因列
filtered_data['对广告位进行竞价操作的原因'] = '满足定义一的所有条件'

# 输出结果
output_columns = [
    'campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d',
    'total_clicks_7d', 'total_clicks_3d', '竞价调整', '对广告位进行竞价操作的原因'
]

# 保存到CSV文件
filtered_data[output_columns].to_csv(
    r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_IT_2024-06-13_deepseek.csv',
    index=False
)

print("结果已保存到CSV文件。")