# filename: find_poor_performance_campaigns.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义条件
def is_definition_one(row):
    return (row['ACOS7d'] > 0.24 and row['ACOSYesterday'] > 0.24 and row['costYesterday'] > 5.5 and 
            row['ACOS30d'] > row['countryAvgACOS1m'])

def is_definition_two(row):
    return (row['ACOS30d'] > 0.24 and row['ACOS30d'] > row['countryAvgACOS1m'] and 
            row['totalSales7d'] == 0 and row['totalCost7d'] > 10)

# 空列表存储结果
results = []

# 遍历每一行数据，进行条件判断和预算调整
for index, row in data.iterrows():
    definition_one = is_definition_one(row)
    definition_two = is_definition_two(row)
    new_budget = row['campaignBudget']
    reason = None
    
    if definition_one:
        if row['campaignBudget'] > 13:
            new_budget = max(8, row['campaignBudget'] - 5)
            reason = '定义一'
        elif row['campaignBudget'] < 8:
            new_budget = row['campaignBudget']
    
    if definition_two:
        new_budget = max(5, row['campaignBudget'] - 5)
        reason = '定义二'

    if reason:
        results.append([
            row['campaignId'],
            row['campaignName'],
            row['campaignBudget'],
            new_budget,
            row['clicksYesterday'],
            row['ACOSYesterday'],
            row['ACOS7d'],
            row['totalClicks7d'],
            row['totalSales7d'],
            row['ACOS30d'],
            row['totalClicks30d'],
            row['totalSales30d'],
            row['countryAvgACOS1m'],
            reason
        ])

# 转换为DataFrame并保存到CSV文件
columns = [
    'campaignId', 'campaignName', 'Budget', 'New Budget', 'clicks', 'ACOSYesterday',
    'ACOS7d', 'totalClicks7d', 'totalSales7d', 'ACOS30d', 'totalClicks30d', 
    'totalSales30d', 'countryAvgACOS1m', 'Reason'
]
results_df = pd.DataFrame(results, columns=columns)

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_IT_2024-07-13.csv'
results_df.to_csv(output_path, index=False)

print("结果已保存到", output_path)