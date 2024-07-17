# filename: check_bad_campaigns_def2.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义二：符合条件的广告活动
def filter_bad_campaigns_def2(row):
    return (
        row['ACOS30d'] > 0.24 and
        row['ACOS30d'] > row['countryAvgACOS1m'] and
        row['totalSales7d'] == 0 and
        row['totalCost7d'] > 10
    )

# 输出符合定义二条件的广告活动
bad_campaigns_def2 = data[data.apply(filter_bad_campaigns_def2, axis=1)]

# 打印符合条件的广告活动
print(bad_campaigns_def2)
print(f"Number of campaigns meeting definition 2: {len(bad_campaigns_def2)}")