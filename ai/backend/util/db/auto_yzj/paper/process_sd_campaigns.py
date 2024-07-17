# filename: process_sd_campaigns.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合条件的广告活动
filtered_data = data[
    (data['ACOS7d'] < 0.24) & 
    (data['ACOSYesterday'] < 0.24) & 
    (data['costYesterday'] > 0.8 * data['campaignBudget'])
]

# 更新预算
filtered_data['NewBudget'] = filtered_data['campaignBudget']
filtered_data['Reason'] = 'ACOS in last 7 days and yesterday below 0.24, and cost yesterday exceeded 80% of the budget'

for index, row in filtered_data.iterrows():
    while row['NewBudget'] < 50:
        row['NewBudget'] *= 1.2

# 生成需要输出的DataFrame
output_data = filtered_data[['campaignId', 'campaignName', 'campaignBudget', 'NewBudget',
                             'costYesterday', 'clicksYesterday', 'ACOSYesterday',
                             'ACOS7d', 'countryAvgACOS1m', 'totalClicks30d', 'totalSales30d', 'Reason']]

# 保存至指定的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_ES_2024-07-16.csv'
output_data.to_csv(output_path, index=False)

print("筛选并保存结果成功！")