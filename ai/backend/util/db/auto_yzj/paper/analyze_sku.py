# filename: analyze_sku.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv'
df = pd.read_csv(file_path)

# 条件筛选
result_df = df[
    (
        (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)
    ) | (
        (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)
    ) | (
        (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)
    ) | (
        (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
    ) | (
        (df['ACOS_7d'] > 0.5)
    ) | (
        (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)
    )
][['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku']]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\手动_关闭SKU_v1_1_ES_2024-06-12.csv'
result_df.to_csv(output_path, index=False)

print('筛选并保存完成')