# filename: custom_condition_verification.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 放宽条件筛选广告活动
def filter_custom_bad_campaigns(row):
    return (
        row['ACOS7d'] > 0.1 or  # 放宽条件
        row['ACOSYesterday'] > 0.1 or  # 放宽条件
        row['costYesterday'] > 3  # 放宽条件
    )

# 输出符合放宽条件的广告活动
custom_bad_campaigns = data[data.apply(filter_custom_bad_campaigns, axis=1)]

# 打印符合条件的广告活动
print(custom_bad_campaigns)
print(f"Number of campaigns meeting custom conditions: {len(custom_bad_campaigns)}")