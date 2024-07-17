# filename: process_and_output_results.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
data = pd.read_csv(file_path)

# 定义函数来判断并标记劣质广告活动及其预算调整
def evaluate_campaign(row):
    reasons = []

    # 判断定义一
    if (row['ACOS7d'] > 0.24 and
        row['ACOSYesterday'] > 0.24 and
        row['costYesterday'] > 5.5 and
        row['ACOS30d'] > row['countryAvgACOS1m']):
        if row['campaignBudget'] > 13:
            new_budget = max(row['campaignBudget'] - 5, 8)
        else:
            new_budget = row['campaignBudget']
        reasons.append("定义一")

    # 判断定义二
    elif (row['ACOS30d'] > 0.24 and
          row['ACOS30d'] > row['countryAvgACOS1m'] and
          row['totalSales7d'] == 0 and
          row['totalCost7d'] > 10):
        new_budget = max(row['campaignBudget'] - 5, 5)
        reasons.append("定义二")

    # 如果满足定义条件则返回相应结果
    if reasons:
        return pd.Series([row['campaignId'], row['campaignName'], row['campaignBudget'], new_budget,
                          row['clicksYesterday'], row['ACOSYesterday'], row['ACOS7d'], row['totalClicks7d'],
                          row['totalSales7d'], row['ACOS30d'], row['totalClicks30d'], row['totalSales30d'], 
                          row['countryAvgACOS1m'], ", ".join(reasons)])
    return pd.Series([None] * 14)

# 应用函数并过滤出劣质广告活动
columns = ['campaignId', 'campaignName', 'Campaign Budget', 'New Budget', 'Clicks', 'Yesterday_ACOS', 
           'ACOS7d', 'Total Clicks 7d', 'Total Sales 7d', 'ACOS30d', 'Total Clicks 30d', 
           'Total Sales 30d', 'Country Avg ACOS 1m', 'Reason']

# 使用apply函数来逐行应用我们的评估函数，并过滤掉未匹配的行
result_data = data.apply(evaluate_campaign, axis=1)
result_data.columns = columns
result_data = result_data.dropna()

# 保存结果到CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_DE_2024-07-16.csv"
result_data.to_csv(output_file_path, index=False)

# 打印保存成功的信息
print(f"结果已保存到 {output_file_path}")