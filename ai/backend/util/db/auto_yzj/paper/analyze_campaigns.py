# filename: analyze_campaigns.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
data = pd.read_csv(file_path)

# 筛选定义一中的广告活动
def1_campaigns = data[(data['ACOS7d'] > 0.24) & (data['ACOSYesterday'] > 0.24) & 
                      (data['costYesterday'] > 5.5) & 
                      (data['ACOS30d'] > data['countryAvgACOS1m'])]

# 对定义一中的广告活动进行预算调整
def1_campaigns['New Budget'] = def1_campaigns['campaignBudget'].apply(
    lambda x: max(x - 5, 8) if x > 13 else x
)

# 筛选定义二中的广告活动
def2_campaigns = data[(data['ACOS30d'] > 0.24) & (data['ACOS30d'] > data['countryAvgACOS1m']) & 
                      (data['totalSales7d'] == 0) & (data['totalCost7d'] > 10)]

# 对定义二中的广告活动进行预算调整
def2_campaigns['New Budget'] = def2_campaigns['campaignBudget'].apply(
    lambda x: max(x - 5, 5) if x > 5 else x
)

# 合并筛选结果
result_campaigns = pd.concat([def1_campaigns, def2_campaigns])
result_campaigns['原因'] = result_campaigns.apply(
    lambda row: '定义一' if row.name in def1_campaigns.index else '定义二', axis=1
)

# 选择所需的列
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'New Budget', 'clicksYesterday', 
    'ACOSYesterday', 'ACOS7d', 'totalClicks7d', 'totalSales7d', 'ACOS30d', 
    'totalClicks30d', 'totalSales30d', 'countryAvgACOS1m', '原因'
]
output_df = result_campaigns[output_columns]

# 保存结果
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_DE_2024-07-14.csv"
output_df.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")