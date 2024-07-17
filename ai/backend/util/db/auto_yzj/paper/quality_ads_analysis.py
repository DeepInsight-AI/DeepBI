# filename: quality_ads_analysis.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义过滤条件
conditions = [
    # 定义一
    (df['ACOS7d'] > 0.24) & 
    (df['ACOSYesterday'] > 0.24) & 
    (df['costYesterday'] > 5.5) & 
    (df['ACOS30d'] > df['countryAvgACOS1m']),
    
    # 定义二
    (df['ACOS30d'] > 0.24) & 
    (df['ACOS30d'] > df['countryAvgACOS1m']) & 
    (df['totalSales7d'] == 0) & 
    (df['totalCost7d'] > 10)
]

# 将广告活动标记为要调整预算
df['adjustReason'] = ''
df['newBudget'] = df['campaignBudget']  # 初始化 newBudget 列

df.loc[conditions[0], 'adjustReason'] = '定义一'
df.loc[conditions[1], 'adjustReason'] = '定义二'

# 定义预算调整逻辑
def adjust_budget(row):
    if row['adjustReason'] == '定义一' and row['campaignBudget'] > 13:
        row['newBudget'] = max(8, row['campaignBudget'] - 5)
    elif row['adjustReason'] == '定义一' and row['campaignBudget'] < 8:
        row['newBudget'] = row['campaignBudget']
    elif row['adjustReason'] == '定义二' and row['campaignBudget'] > 5:
        row['newBudget'] = max(5, row['campaignBudget'] - 5)
    return row

# 进行预算调整并过滤只需要看到的行
df = df.apply(adjust_budget, axis=1)
result = df[df['adjustReason'] != '']

output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'newBudget',
    'clicksYesterday', 'ACOSYesterday', 'ACOS7d', 
    'totalClicks7d', 'totalSales7d', 'ACOS30d', 
    'totalClicks30d', 'totalSales30d', 'countryAvgACOS1m', 'adjustReason'
]
result = result[output_columns]

# 输出到CSV文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_IT_2024-07-16.csv'
result.to_csv(output_file, index=False)

print("任务完成，结果已保存至CSV文件.")