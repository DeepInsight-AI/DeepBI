# filename: SD_poor_performance_campaign_adjustment.py

import pandas as pd

def adjust_budget(row):
    reasons = []
    new_budget = row['campaignBudget']

    # 定义一条件
    if row['ACOS7d'] > 0.24 and row['ACOSYesterday'] > 0.24 and row['costYesterday'] > 5.5 and row['ACOS30d'] > row['countryAvgACOS1m']:
        if row['campaignBudget'] > 13:
            new_budget = max(row['campaignBudget'] - 5, 8)
        reasons.append("定义一")

    # 定义二条件
    if row['ACOS30d'] > 0.24 and row['ACOS30d'] > row['countryAvgACOS1m'] and row['totalSales7d'] == 0 and row['totalCost7d'] > 10:
        new_budget = max(row['campaignBudget'] - 5, 5)
        reasons.append("定义二")

    return new_budget, '; '.join(reasons)

# 读取CSV文件
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/sd广告/预算优化/预处理.csv'
data = pd.read_csv(file_path)

# 过滤出需要调整预算的广告活动
data['NewBudget'], data['原因'] = zip(*data.apply(adjust_budget, axis=1))

# 结果保存
output_file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/sd广告/预算优化/提问策略/SD_劣质sd广告活动_v1_1_LAPASA_ES_2024-07-15.csv'
result = data[['campaignId', 'campaignName', 'campaignBudget', 'NewBudget', 'clicksYesterday', 'ACOS7d', 'totalClicks7d', 'totalSales7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d', 'countryAvgACOS1m', '原因']]
result.to_csv(output_file_path, index=False)
print(f'Results are saved to {output_file_path}')