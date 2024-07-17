# filename: process_bad_campaigns_final.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义一：符合条件的广告活动
def filter_bad_campaigns_def1(row):
    return (
        row['ACOS7d'] > 0.24 and
        row['ACOSYesterday'] > 0.24 and
        row['costYesterday'] > 5.5 and
        row['ACOS30d'] > row['countryAvgACOS1m']
    )

# 对定义一的广告活动调整预算
def adjust_budget_def1(row):
    if row['campaignBudget'] > 13:
        new_budget = max(8, row['campaignBudget'] - 5)
    else:
        new_budget = row['campaignBudget']
    return new_budget

# 定义二：符合条件的广告活动
def filter_bad_campaigns_def2(row):
    return (
        row['ACOS30d'] > 0.24 and
        row['ACOS30d'] > row['countryAvgACOS1m'] and
        row['totalSales7d'] == 0 and
        row['totalCost7d'] > 10
    )

# 对定义二的广告活动调整预算
def adjust_budget_def2(row):
    new_budget = max(5, row['campaignBudget'] - 5)
    return new_budget

# 创建一个新的DataFrame存储符合条件的广告活动及其理由
output_data = []
for _, row in data.iterrows():
    reason = None
    new_budget = row['campaignBudget']
    
    if filter_bad_campaigns_def1(row):
        new_budget = adjust_budget_def1(row)
        reason = '定义一'
    
    if filter_bad_campaigns_def2(row):
        new_budget = adjust_budget_def2(row)
        reason = '定义二'
    
    if reason:
        output_data.append([
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

output_df = pd.DataFrame(output_data, columns=[
    'campaignId',
    'campaignName',
    'Budget',
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
])

# 将结果输出到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_DE_2024-07-12.csv'
output_df.to_csv(output_file_path, index=False)

# 打印一下输出的行数，方便判断结果是否正确
print(f"Number of bad campaigns identified: {len(output_df)}")