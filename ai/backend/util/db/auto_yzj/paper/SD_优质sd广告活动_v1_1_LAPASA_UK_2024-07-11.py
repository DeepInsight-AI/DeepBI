# filename: SD_优质sd广告活动_v1_1_LAPASA_UK_2024-07-11.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
data = pd.read_csv(file_path)

# 查看基本统计信息和分布情况
print(data.describe())
print(data[['ACOS7d', 'ACOSYesterday', 'costYesterday', 'campaignBudget']].head())  # 查看部分数据前5行

# 筛选表现很好的优质广告活动条件
condition = (data['ACOS7d'] < 0.24) & \
            (data['ACOSYesterday'] < 0.24) & \
            (data['costYesterday'] > 0.8 * data['campaignBudget'])

# 复制满足条件的数据
good_campaigns = data[condition].copy()

# 增加预算并记录原因
good_campaigns['New Budget'] = good_campaigns['campaignBudget'] * 1.2
good_campaigns['New Budget'] = good_campaigns['New Budget'].apply(lambda x: 50 if x > 50 else x)
good_campaigns['理由'] = "增加原来预算的0.2倍，直到预算为50"

# 输出调试信息
print(good_campaigns.head())  # 确认字段以及部分内容

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