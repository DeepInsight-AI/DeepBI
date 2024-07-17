# filename: sd_ad_optimization.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义一：筛选符合条件的广告活动
def filter_by_def_1(df):
    filtered_df = df[
        (df['ACOS7d'] > 0.24) & 
        (df['ACOSYesterday'] > 0.24) & 
        (df['costYesterday'] > 5.5) & 
        (df['ACOS30d'] > df['countryAvgACOS1m'])
    ]
    
    # 进行预算调整
    def adjust_budget_1(row):
        if row['campaignBudget'] > 13:
            new_budget = max(8, row['campaignBudget'] - 5)
        elif row['campaignBudget'] < 8:
            new_budget = row['campaignBudget']
        else:
            new_budget = row['campaignBudget']
        return new_budget
    
    filtered_df['NewBudget'] = filtered_df.apply(adjust_budget_1, axis=1)
    filtered_df['Reason'] = '定义一'
    return filtered_df

# 定义二：筛选符合条件的广告活动
def filter_by_def_2(df):
    filtered_df = df[
        (df['ACOS30d'] > 0.24) & 
        (df['ACOS30d'] > df['countryAvgACOS1m']) &
        (df['totalSales7d'] == 0) & 
        (df['totalCost7d'] > 10)
    ]
    
    # 进行预算调整
    def adjust_budget_2(row):
        new_budget = max(5, row['campaignBudget'] - 5)
        return new_budget
    
    filtered_df['NewBudget'] = filtered_df.apply(adjust_budget_2, axis=1)
    filtered_df['Reason'] = '定义二'
    return filtered_df

# 筛选并合并结果
def get_low_performance_campaigns(df):
    result_df_list = []
    result_df_list.append(filter_by_def_1(df))
    result_df_list.append(filter_by_def_2(df))
    result_df = pd.concat(result_df_list, ignore_index=True)
    return result_df

# 获取筛选结果
low_performance_df = get_low_performance_campaigns(df)

# 重命名列
low_performance_df.rename(columns={
    'campaignId': 'CampaignID',
    'campaignName': 'CampaignName',
    'campaignBudget': 'Budget',
    'NewBudget': 'New Budget',
    'ACOS7d': 'ACOS7d',
    'totalClicks7d': 'Clicks7d',
    'totalSales7d': 'Sales7d',
    'ACOS30d': 'ACOS30d',
    'totalClicks30d': 'Clicks30d',
    'totalSales30d': 'Sales30d',
    'countryAvgACOS1m': 'CountryAvgACOS1m',
    'Reason': 'Reason'
}, inplace=True)

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_FR_2024-07-14.csv'
low_performance_df.to_csv(output_file_path, index=False)

print(f"Results saved to {output_file_path}")