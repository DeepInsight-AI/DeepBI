# filename: 调整预算.py

import pandas as pd

# 文件路径
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_DE_2024-07-11.csv'

# 读取CSV文件
data = pd.read_csv(input_file_path)

# 筛选表现优质的广告活动
filtered_data = data[(data['ACOS7d'] < 0.24) & 
                     (data['ACOSYesterday'] < 0.24) & 
                     (data['costYesterday'] > 0.8 * data['campaignBudget'])]

# 为筛选的广告活动增加预算
filtered_data['newBudget'] = filtered_data['campaignBudget'].apply(lambda x: min(x * 1.2, 50))

# 准备要输出的字段
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'newBudget', 'costYesterday', 'clicksYesterday', 
    'ACOSYesterday', 'ACOS7d', 'countryAvgACOS1m', 'totalClicks30d', 'totalSales30d'
]

# 增加原因字段
filtered_data['reason'] = '优质广告活动，符合条件增加预算'

# 输出结果到新的CSV文件
filtered_data[output_columns + ['reason']].to_csv(output_file_path, index=False)

# 输出成功提示
print(f"筛选并调整后的广告活动已保存到 {output_file_path}")