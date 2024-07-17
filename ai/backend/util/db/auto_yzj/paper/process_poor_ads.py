# filename: process_poor_ads.py

import pandas as pd

# 函数定义
def poor_ad_judgment(df):
    df['New Budget'] = df['campaignBudget']  # 初始化新的预算列
    df['Reason'] = ""  # 初始化原因列

    # 定义一的过滤条件
    condition1 = (
        (df['ACOS7d'] > 0.24) &
        (df['ACOSYesterday'] > 0.24) &
        (df['costYesterday'] > 5.5) &
        (df['ACOS30d'] > df['countryAvgACOS1m'])
    )

    # 对满足条件一的广告活动进行预算调整
    df.loc[condition1 & (df['campaignBudget'] > 13), 'New Budget'] = df.loc[condition1 & (df['campaignBudget'] > 13), 'campaignBudget'] - 5
    df.loc[condition1 & (df['campaignBudget'] <= 8), 'New Budget'] = df.loc[condition1 & (df['campaignBudget'] <= 8), 'campaignBudget']
    df.loc[condition1, 'Reason'] = "定义一"

    # 定义二的过滤条件
    condition2 = (
        (df['ACOS30d'] > 0.24) &
        (df['ACOS30d'] > df['countryAvgACOS1m']) &
        (df['totalSales7d'] == 0) &
        (df['totalCost7d'] > 10)
    )

    # 对满足条件二的广告活动进行预算调整
    df.loc[condition2, 'New Budget'] = df['campaignBudget'] - 5
    df.loc[condition2 & (df['New Budget'] < 5), 'New Budget'] = 5
    df.loc[condition2, 'Reason'] = "定义二"

    return df

# 读取CSV文件
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\预处理.csv'
df = pd.read_csv(file_path)

# 应用过滤条件函数
df = poor_ad_judgment(df)

# 保存结果到新的CSV文件中
output_file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\提问策略\\SD_劣质sd广告活动_v1_1_LAPASA_UK_2024-07-16.csv'
df.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")