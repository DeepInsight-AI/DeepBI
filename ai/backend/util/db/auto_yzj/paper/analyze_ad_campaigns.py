# filename: analyze_ad_campaigns.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_FR_2024-07-14.csv'
data = pd.read_csv(file_path)

# 定义一的条件
condition1 = (
    (data['ACOS7d'] > 0.24) &
    (data['ACOSYesterday'] > 0.24) &
    (data['costYesterday'] > 5.5) &
    (data['ACOS30d'] > data['countryAvgACOS1m'])
)

# 定义二的条件
condition2 = (
    (data['ACOS30d'] > 0.24) &
    (data['ACOS30d'] > data['countryAvgACOS1m']) &
    (data['totalSales7d'] == 0) &
    (data['totalCost7d'] > 10)
)

# 找出符合任一条件的广告活动
poor_campaigns = data[condition1 | condition2].copy()

# 调整预算并添加新列 "New Budget" 和 "Reason"
def adjust_budget(row):
    new_budget = row['campaignBudget']
    reason = ""

    if condition1[row.name]:
        if row['campaignBudget'] > 13:
            new_budget = max(row['campaignBudget'] - 5, 8)
        elif row['campaignBudget'] <= 8:
            new_budget = row['campaignBudget']
        reason = "定义一"
    elif condition2[row.name]:
        new_budget = max(row['campaignBudget'] - 5, 5)
        reason = "定义二"

    if isinstance(new_budget, (int, float)) and new_budget < 5:
        new_budget = "关闭"

    return new_budget, reason

# 调用 adjust_budget 函数并创建新列
results = poor_campaigns.apply(adjust_budget, axis=1, result_type='expand')

# 调试输出每一项
print("调试信息 - results 类型:", type(results))
print("调试信息 - results 前几行:\n", results.head())

# 检查 results 格式，并分开赋值
if results.shape[1] == 2:
    new_budgets, reasons = zip(*results.values)
    # 将新列加入到DataFrame中
    poor_campaigns['New Budget'] = new_budgets
    poor_campaigns['Reason'] = reasons

    # 选择需要输出的列
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
        'Reason'
    ]
    output_data = poor_campaigns[output_columns]

    # 输出结果到CSV文件
    output_data.to_csv(output_file_path, index=False)

    print("结果已保存到文件:", output_file_path)
else:
    print("结果格式不符合预期：", results.head())