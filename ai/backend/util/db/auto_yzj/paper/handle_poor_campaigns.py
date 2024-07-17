# filename: handle_poor_campaigns.py

import pandas as pd

# 数据加载
data_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\预处理.csv"
df = pd.read_csv(data_path)

# 定义一
condition1 = (
    (df['ACOS7d'] > 0.24) &
    (df['ACOSYesterday'] > 0.24) &
    (df['costYesterday'] > 5.5) &
    (df['ACOS30d'] > df['countryAvgACOS1m'])
)

# 定义二
condition2 = (
    (df['ACOS30d'] > 0.24) &
    (df['ACOS30d'] > df['countryAvgACOS1m']) &
    (df['totalSales7d'] == 0) &
    (df['totalCost7d'] > 10)
)

# 选择符合条件的行
poor_campaigns = df[condition1 | condition2].copy()

# 初始化新的预算列
poor_campaigns['New Budget'] = poor_campaigns['campaignBudget']

# 定义操作
def adjust_budget(row):
    if row['campaignBudget'] > 13:
        if row['campaignBudget'] - 5 < 8:
            row['New Budget'] = 8
        else:
            row['New Budget'] = row['campaignBudget'] - 5
    elif condition2 and row['campaignBudget'] - 5 < 5:
        row['New Budget'] = 5
    return row

# 应用预算调整操作
poor_campaigns = poor_campaigns.apply(adjust_budget, axis=1)

# 添加原因列
poor_campaigns['Reason'] = None
poor_campaigns.loc[condition1 & ~condition2, 'Reason'] = '定义一'
poor_campaigns.loc[condition2 & ~condition1, 'Reason'] = '定义二'
poor_campaigns.loc[condition1 & condition2, 'Reason'] = '定义一和定义二'

# 选择输出的列
output_columns = ['campaignId', 'campaignName', 'campaignBudget', 'New Budget',
                  'clicksYesterday', 'ACOSYesterday', 'ACOS7d', 'totalClicks7d', 
                  'totalSales7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d', 
                  'countryAvgACOS1m', 'Reason']

output_df = poor_campaigns[output_columns]

# 保存结果
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\提问策略\\SD_劣质sd广告活动_v1_1_LAPASA_DE_2024-07-11.csv"
output_df.to_csv(output_path, index=False)

print("处理完成，结果已保存到CSV文件")