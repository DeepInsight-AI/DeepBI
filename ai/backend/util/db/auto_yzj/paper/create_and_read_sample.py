# filename: create_and_read_sample.py
import pandas as pd

# 创建一个示例CSV文件
sample_data = {
    "campaignName": ["Campaign 1", "Campaign 2"],
    "campaignId": [1, 2],
    "market": ["US", "UK"],
    "total_cost_7d": [100, 150],
    "total_sales14d_7d": [200, 300],
    "total_cost_30d": [400, 500],
    "total_sales14d_30d": [600, 700],
    "total_clicks_30d": [50, 60],
    "total_clicks_7d": [10, 15],
    "ACOS_30d": [0.25, 0.30],
    "ACOS_7d": [0.26, 0.32],
    "ACOS_yesterday": [0.27, 0.35],
    "country_avg_ACOS_1m": [0.20, 0.22]
}

sample_df = pd.DataFrame(sample_data)
sample_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\sample_preprocess.csv'
sample_df.to_csv(sample_file_path, index=False, encoding='utf-8-sig')

# 读取示例CSV文件
data = pd.read_csv(sample_file_path, encoding='utf-8-sig')
print(data.head())
print(f'Sample file loaded successfully with {data.shape[0]} rows and {data.shape[1]} columns.')