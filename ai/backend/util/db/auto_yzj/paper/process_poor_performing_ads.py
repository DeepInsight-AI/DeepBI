# filename: process_poor_performing_ads.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 为了节省计算资源，只保留需要的字段
fields = [
    "placementClassification",
    "campaignName",
    "total_clicks_3d",
    "total_clicks_7d",
    "total_sales14d_7d",
    "ACOS_3d",
    "ACOS_7d"
]
data = data[fields]

print(data.head())