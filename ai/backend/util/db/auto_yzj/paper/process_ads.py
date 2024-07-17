# filename: process_ads.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义一的条件
def filter_condition_one(row):
    return (row['ACOS7d'] > 0.24 and
            row['ACOSYesterday'] > 0.24 and
            row['costYesterday'] > 5.5 and
            row['ACOS30d'] > row['countryAvgACOS1m'])

# 定义二的条件
def filter_condition_two(row):
    return (row['ACOS30d'] > 0.24 and
            row['ACOS30d'] > row['countryAvgACOS1m'] and
            row['totalSales7d'] == 0 and
            row['totalCost7d'] > 10)

# 判断是否需要降低预算并计算新预算
def adjust_budget_one(row):
    if row['campaignBudget'] > 13:
        new_budget = max(row['campaignBudget'] - 5, 8)
    elif row['campaignBudget'] < 8:
        new_budget = row['campaignBudget']
    else:
        new_budget = row['campaignBudget']
    return new_budget

def adjust_budget_two(row):
    new_budget = max(row['campaignBudget'] - 5, 5)
    return new_budget

# 过滤并调整预算
results = []
for index, row in df.iterrows():
    filter_one = filter_condition_one(row)
    filter_two = filter_condition_two(row)
    
    if filter_one:
        new_budget = adjust_budget_one(row)
        reason = "定义一"
    elif filter_two:
        new_budget = adjust_budget_two(row)
        reason = "定义二"
    else:
        continue
    
    result = {
        "campaignId": row["campaignId"],
        "campaignName": row["campaignName"],
        "Original Budget": row["campaignBudget"],
        "New Budget": new_budget,
        "Clicks Yesterday": row["clicksYesterday"],
        "ACOS Yesterday": row["ACOSYesterday"],
        "7d ACOS": row["ACOS7d"],
        "7d Clicks": row["totalClicks7d"],
        "7d Sales": row["totalSales7d"],
        "30d ACOS": row["ACOS30d"],
        "30d Clicks": row["totalClicks30d"],
        "30d Sales": row["totalSales30d"],
        "Country Avg ACOS (1m)": row["countryAvgACOS1m"],
        "Reason": reason
    }
    results.append(result)

# 转换为DataFrame并保存为CSV
results_df = pd.DataFrame(results)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_UK_2024-07-15.csv'
results_df.to_csv(output_path, index=False)
print("结果已保存至:", output_path)