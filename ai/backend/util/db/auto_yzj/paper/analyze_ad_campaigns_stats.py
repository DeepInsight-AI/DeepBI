# filename: analyze_ad_campaigns_stats.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 打印数据的统计描述信息
print("数据的统计描述信息:\n", data.describe())

# 打印部分条件的基础信息
print("\nACOS7d的值分布:\n", data['ACOS7d'].describe())
print("\nACOSYesterday的值分布:\n", data['ACOSYesterday'].describe())
print("\ncostYesterday的值分布:\n", data['costYesterday'].describe())
print("\nACOS30d的值分布:\n", data['ACOS30d'].describe())
print("\ncountryAvgACOS1m的值分布:\n", data['countryAvgACOS1m'].describe())
print("\ntotalSales7d的值分布:\n", data['totalSales7d'].describe())
print("\ntotalCost7d的值分布:\n", data['totalCost7d'].describe())