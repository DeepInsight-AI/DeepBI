# filename: poor_ads_campaign_analysis.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 打印初始数据
print("Initial data:")
print(data.head())

# 定义条件一
condition1 = (
    (data['ACOS7d'] > 0.24) &
    (data['ACOSYesterday'] > 0.24) &
    (data['costYesterday'] > 5.5) &
    (data['ACOS30d'] > data['countryAvgACOS1m'])
)

# 定义条件二
condition2 = (
    (data['ACOS30d'] > 0.24) &
    (data['ACOS30d'] > data['countryAvgACOS1m']) &
    (data['totalSales7d'] == 0) &
    (data['totalCost7d'] > 10)
)

# 筛选符合条件的广告活动
poor_campaigns = data[condition1 | condition2].copy()
print("Filtered poor campaigns:")
print(poor_campaigns.head())

# 调整预算函数
def adjust_budget(row):
    reason = []
    new_budget = row['campaignBudget']

    if condition1.loc[row.name]:
        reason.append("定义一")
        if row['campaignBudget'] > 13:
            new_budget = max(row['campaignBudget'] - 5, 8)
    if condition2.loc[row.name]:
        reason.append("定义二")
        if row['campaignBudget'] > 5:
            new_budget = max(row['campaignBudget'] - 5, 5)

    return pd.Series([new_budget, ','.join(reason)], index=['NewBudget', 'Reason'])

# 应用调整预算函数
adjusted_values = poor_campaigns.apply(adjust_budget, axis=1)
print("Adjusted values DataFrame:")
print(adjusted_values.head())

# 检查列名
print("Columns of adjusted_values:", adjusted_values.columns)

# 合并调整后的数据
poor_campaigns = pd.concat([poor_campaigns, adjusted_values], axis=1)
print("Combined DataFrame with NewBudget and Reason:")
print(poor_campaigns.head())

# 确认新列被添加
print("Columns of poor_campaigns after merge:", poor_campaigns.columns)

# 转换关闭广告活动预算
# 添加额外的检查是否存在新预算列
if 'NewBudget' in poor_campaigns.columns:
    poor_campaigns['NewBudget'] = poor_campaigns['NewBudget'].apply(lambda x: '关闭' if x != '关闭' and x < 5 else x)
else:
    print("Error: 'NewBudget' column not found!")

# 打印调试信息
print("DataFrame after budget closure criteria:")
print(poor_campaigns.head())

# 输出结果
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'NewBudget', 'clicksYesterday', 'ACOS7d', 
    'totalClicks7d', 'totalSales7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d', 
    'countryAvgACOS1m', 'Reason'
]

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_DE_2024-07-15.csv'
poor_campaigns.to_csv(output_path, columns=output_columns, index=False)

print("Analysis complete. Results saved to:", output_path)