# filename: SD_劣质sd广告活动_v1_1_LAPASA_FR_2024-07-15.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义一筛选条件
condition1 = (
    (data['ACOS7d'] > 0.24) & 
    (data['ACOSYesterday'] > 0.24) &
    (data['costYesterday'] > 5.5) &
    (data['ACOS30d'] > data['countryAvgACOS1m'])
)

# 筛选符合定义一条件的广告活动
df_definition1 = data[condition1].copy()

# 调整预算
df_definition1['New Budget'] = df_definition1['campaignBudget'].apply(
    lambda x: max(8, x-5) if x > 13 else x
)

# 标识降预算的原因
df_definition1['原因'] = '符合定义一，预算降低'

# 定义二筛选条件
condition2 = (
    (data['ACOS30d'] > 0.24) & 
    (data['ACOS30d'] > data['countryAvgACOS1m']) &
    (data['totalSales7d'] == 0) &
    (data['totalCost7d'] > 10)
)

# 筛选符合定义二条件的广告活动
df_definition2 = data[condition2].copy()

# 调整预算
df_definition2['New Budget'] = df_definition2['campaignBudget'].apply(
    lambda x: max(5, x-5)
)

# 标识降预算的原因
df_definition2['原因'] = '符合定义二，预算降低'

# 合并两个定义的结果
df_result = pd.concat([df_definition1, df_definition2])

# 选择输出字段
output_columns = [
    'campaignId',
    'campaignName',
    'campaignBudget',
    'New Budget',
    'clicksYesterday',
    'ACOSYesterday',
    'ACOS7d',
    'totalClicks7d',
    'totalSales7d',
    'ACOS30d',
    'totalClicks30d',
    'totalSales30d',
    'countryAvgACOS1m',
    '原因'
]

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_FR_2024-07-15.csv'
df_result.to_csv(output_file_path, index=False, columns=output_columns, encoding='utf-8-sig')

print("操作完成，结果已保存到CSV文件中！")