# filename: process_campaign_performance.py
import pandas as pd
import numpy as np

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选符合条件的广告活动
filtered_df = df[(df['ACOS_7d'] < 0.27) & 
                 (df['ACOS_yesterday'] < 0.27) & 
                 (df['cost_yesterday'] > 0.8 * df['Budget'])]

# 增加预算, 直到Budget为50
filtered_df['New_Budget'] = filtered_df['Budget'].apply(lambda x: min(x * 1.2, 50))

# 准备输出的DataFrame
output_df = filtered_df[['campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday', 
                         'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 
                         'country_avg_ACOS_1m', 'total_clicks_30d', 'total_sales14d_30d']]

output_df['Reason'] = '满足定义：最近7天的平均ACOS值在0.27以下，昨天的ACOS值在0.27以下，昨天花费超过了昨天预算的80%'

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_v1_1_LAPASA_FR_2024-07-03.csv'
output_df.to_csv(output_file_path, index=False)

print("处理完毕，结果保存在:", output_file_path)