# filename: filter_and_export.py

import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合条件的数据
filtered_data = data[(data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)]

# 设置原因字段
filtered_data['reason'] = "近七天有销售额且ACOS值低于0.2"

# 选择需要的字段
filtered_data = filtered_data[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'matchType', 'reason']]
filtered_data.columns = ['Campaign Name', 'adGroupName', 'week_acos', 'searchTerm', 'matchtype', 'reason']

# 保存到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\优质搜索词_FR.csv'
filtered_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"Filtered data saved to {output_path}")