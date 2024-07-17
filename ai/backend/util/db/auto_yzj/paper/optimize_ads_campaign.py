# filename: optimize_ads_campaign.py

import pandas as pd

# 读取CSV文件
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_ES_2024-07-13.csv'

# 加载数据
df = pd.read_csv(data_path)

# 创建一个新的DataFrame来保存识别出的劣质广告活动
results = []

# 遍历DataFrame中的每一个广告活动
for index, row in df.iterrows():
    updated_budget = row['campaignBudget']
    close_campaign = False
    reason = ""
    
    # 定义一的条件
    if (row['ACOS7d'] > 0.24 and row['ACOSYesterday'] > 0.24 and
        row['costYesterday'] > 5.5 and row['ACOS30d'] > row['countryAvgACOS1m']):
        reason = "定义一: 低效广告活动"
        if row['campaignBudget'] > 13:
            if row['campaignBudget'] - 5 < 8:
                updated_budget = 8
            else:
                updated_budget -= 5
        # 如果定义一和其他定义也符合，并将优先关闭。
        if row['campaignBudget'] < 8:
            updated_budget = row['campaignBudget']
    # 定义二的条件
    elif (row['ACOS30d'] > 0.24 and row['ACOS30d'] > row['countryAvgACOS1m'] and
          row['totalSales7d'] == 0 and row['totalCost7d'] > 10):
        reason = "定义二: 低效广告活动"
        if row['campaignBudget'] > 5:
            if row['campaignBudget'] - 5 < 5:
                updated_budget = 5
            else:
                updated_budget -= 5

    # 注册处理
    results.append({
        'campaignId': row['campaignId'],
        'campaignName': row['campaignName'],
        'Budget': row['campaignBudget'],
        'New Budget': "关闭" if close_campaign else updated_budget,
        'clicks': row['clicksYesterday'],
        'ACOS': row['ACOSYesterday'],
        '最近7天的平均ACOS值': row['ACOS7d'],
        '最近7天的总点击次数': row['totalClicks7d'],
        '最近7天的总销售': row['totalSales7d'],
        '最近一个月的平均ACOS值': row['ACOS30d'],
        '最近一个月的总点击数': row['totalClicks30d'],
        '最近一个月的总销售': row['totalSales30d'],
        'countryAvgACOS1m': row['countryAvgACOS1m'],
        '对广告活动进行降低预算的原因': reason
    })

# 转换结果为DataFrame
results_df = pd.DataFrame(results)

# 保存结果到CSV文件
results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print("结果已经保存到:", output_path)