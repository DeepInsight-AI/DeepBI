# filename: 提取搜索词_v1_1.py

import pandas as pd

# Step 1: 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: 定义筛选条件
cond_def1 = (data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)
cond_def2 = (data['ORDER_1m'] > 0) & (data['ACOS_30d'] < 0.24)

# 应用筛选条件
filtered_data_def1 = data[cond_def1].copy()
filtered_data_def1['reason'] = '定义一'

filtered_data_def2 = data[cond_def2].copy()
filtered_data_def2['reason'] = '定义二'

# 合并符合条件的数据
result_data = pd.concat([filtered_data_def1, filtered_data_def2])

# 选择需要的字段
result_data = result_data[['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'ACOS_7d', 'total_sales14d_7d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']]

# 保存结果到新的CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_优质_ASIN_搜索词_v1_1_DE_2024-06-30.csv'
result_data.to_csv(output_file_path, index=False)

print(f"筛选结果已保存到 {output_file_path}")