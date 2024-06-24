# filename: optimize_ad_placement.py

import pandas as pd

# 读取 CSV 文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
data = pd.read_csv(file_path)

# 强制将列转换为数值类型，并在遇到错误时将值设置为 NaN
data['ACOS_7d'] = pd.to_numeric(data['ACOS_7d'], errors='coerce')
data['ACOS_3d'] = pd.to_numeric(data['ACOS_3d'], errors='coerce')
data['total_clicks_7d'] = pd.to_numeric(data['total_clicks_7d'], errors='coerce')
data['total_clicks_3d'] = pd.to_numeric(data['total_clicks_3d'], errors='coerce')

# 条件1和条件2的过滤
condition1 = (
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 24) &
    (data.groupby('campaignName')['ACOS_7d'].transform('min') == data['ACOS_7d']) &
    (data.groupby('campaignName')['total_clicks_7d'].transform('max') != data['total_clicks_7d'])
)

condition2 = (
    condition1 &
    (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 24) &
    (data.groupby('campaignName')['ACOS_3d'].transform('min') == data['ACOS_3d']) &
    (data.groupby('campaignName')['total_clicks_3d'].transform('max') != data['total_clicks_3d'])
)

# 得到符合条件的广告位
qualified_data = data[condition2].copy()

# 添加新的列以显示竞价操作和原因
qualified_data['竞价操作'] = '提高竞价 5%'
qualified_data['对广告位进行竞价操作的原因'] = '满足定义一的所有条件'

# 选择所需列
output_data = qualified_data[[
    'campaignName', 'placementClassification',
    'ACOS_7d', 'ACOS_3d', 'total_clicks_7d',
    'total_clicks_3d', '竞价操作', '对广告位进行竞价操作的原因'
]]

# 保存到新的 CSV 文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\优质广告位_FR.csv"
output_data.to_csv(output_file_path, index=False)

# 打印成功消息和输出的数据
print("符合条件的广告位已保存到 CSV 文件:", output_file_path)
print(output_data.head())