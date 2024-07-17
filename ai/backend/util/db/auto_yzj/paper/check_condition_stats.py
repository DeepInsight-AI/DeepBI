# filename: check_condition_stats.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 统计信息
condition_columns = ['ACOS7d', 'ACOSYesterday', 'costYesterday', 'ACOS30d', 'countryAvgACOS1m', 'totalSales7d', 'totalCost7d']
condition_stats = data[condition_columns].describe()

print(condition_stats)