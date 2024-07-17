# filename: analyze_budget_optimization.py

import pandas as pd

# 读取CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_v1_1_LAPASA_IT_2024-07-03.csv'
data = pd.read_csv(output_file_path)

# 广告活动数量
campaign_count = data.shape[0]

# 预算变化分析
initial_budget_avg = data['Budget'].mean()
new_budget_avg = data['New Budget'].mean()

# 整体表现
total_clicks_30d = data['total_clicks_30d'].sum()
total_sales_30d = data['total_sales14d_30d'].sum()

# 打印分析结果
print(f"广告活动数量: {campaign_count} 个")
print(f"初始预算平均值: {initial_budget_avg:.2f} 单位")
print(f"新预算平均值: {new_budget_avg:.2f} 单位")
print(f"最近30天的总点击数: {total_clicks_30d} 次")
print(f"最近30天的总销售: {total_sales_30d:.2f} 单位")