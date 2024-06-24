# filename: ad_sku_filter.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选符合条件的SKU
filtered_df = df[
    ((df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)) |
    ((df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)) |
    ((df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)) |
    ((df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)) |
    (df['ACOS_7d'] > 0.5) |
    ((df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0))
]

# 选择需要的列
columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku']
result_df = filtered_df[columns]

# 保存到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\手动_关闭SKU_v1_1_IT_2024-06-17.csv'
result_df.to_csv(output_path, index=False)

print(f"筛选结果已保存到 {output_path}")