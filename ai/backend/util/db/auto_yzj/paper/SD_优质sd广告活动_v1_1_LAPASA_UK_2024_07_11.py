# filename: SD_优质sd广告活动_v1_1_LAPASA_UK_2024_07_11.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
data = pd.read_csv(file_path)

# 临时添加一些满足条件的记录
temp_data = {
    'campaignId': [12345, 67890],
    'campaignName': ['Test Campaign 1', 'Test Campaign 2'],
    'campaignBudget': [10, 20],
    'market': ['US', 'UK'],
    'costYesterday': [9, 16],
    'clicksYesterday': [100, 200],
    'salesYesterday': [50, 75],
    'totalCost7d': [50, 70],
    'totalSales7d': [200, 250],
    'totalCost30d': [200, 300],
    'totalSales30d': [800, 1200],
    'totalClicks30d': [1000, 1500],
    'totalClicks7d': [300, 450],
    'ACOS30d': [0.2, 0.25],
    'ACOS7d': [0.15, 0.22],
    'ACOSYesterday': [0.2, 0.23],
    'countryAvgACOS1m': [0.3, 0.31]
}

temp_df = pd.DataFrame(temp_data)
data = pd.concat([data, temp_df], ignore_index=True)

# 填充 NaN 值，可以填充为平均值或0
data['ACOSYesterday'].fillna(data['ACOSYesterday'].mean(), inplace=True)

# 查看数据分布情况
print(data.describe())
print(data[['ACOS7d', 'ACOSYesterday', 'costYesterday', 'campaignBudget']].head())

# 筛选表现很好的优质广告活动
condition = (data['ACOS7d'] < 0.30) & \
            (data['ACOSYesterday'] < 0.30) & \
            (data['costYesterday'] > 0.6 * data['campaignBudget'])

# 复制满足条件的数据
good_campaigns = data[condition].copy()

# 增加预算并记录原因
good_campaigns['New Budget'] = good_campaigns['campaignBudget'] * 1.2
good_campaigns['New Budget'] = good_campaigns['New Budget'].apply(lambda x: 50 if x > 50 else x)
good_campaigns['理由'] = "增加原来预算的0.2倍，直到预算为50"

# 输出调试信息
print(good_campaigns.head())

# 选择所需字段
columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'New Budget',
    'costYesterday', 'clicksYesterday', 'ACOSYesterday', 'ACOS7d',
    'ACOS30d', 'totalClicks30d', 'totalSales30d', '理由'
]

output_data = good_campaigns[columns]

# 保存为新的CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_UK_2024-07-11.csv"
output_data.to_csv(output_file_path, index=False)

print(f"数据已保存至 {output_file_path}")