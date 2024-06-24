# filename: filter_skus.py
import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv"
df = pd.read_csv(file_path)

# 定义筛选条件
condition_1 = (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)
condition_2 = (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)
condition_3 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)
condition_4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
condition_5 = (df['ACOS_7d'] > 0.5)
condition_6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)

# 综合所有条件
filtered_df = df[condition_1 | condition_2 | condition_3 | condition_4 | condition_5 | condition_6]

# 选择所需的列
output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku']
filtered_df = filtered_df[output_columns]

# 保存到新的CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\手动_关闭SKU_v1_1_ES_2024-06-14.csv"
filtered_df.to_csv(output_file_path, index=False)

print("筛选结果已保存到", output_file_path)