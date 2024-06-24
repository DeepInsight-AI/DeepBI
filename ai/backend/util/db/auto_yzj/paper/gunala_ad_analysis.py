# filename: gunala_ad_analysis.py

import pandas as pd

# 读取CSV文件
csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(csv_path)

# 筛选满足条件的数据
# 定义一：
# 1. 近七天有销售额的搜索词。
# 2. 该搜索词的近七天acos值在0.2以下。

filtered_data = data[(data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)]

# 添加原因列
filtered_data['reason'] = '近七天有销售额且近七天ACOS值小于0.2'

# 提取所需列
result = filtered_data[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'reason']]

# 重命名列
result.columns = ['Campaign Name', 'adGroupName', 'week_acos', 'searchTerm', 'reason']

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_优质搜索词_ES_2024-06-121.csv'
result.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_path}")