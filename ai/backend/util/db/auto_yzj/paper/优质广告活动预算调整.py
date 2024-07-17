# filename: 优质广告活动预算调整.py

import pandas as pd

# 定义读取的csv文件路径和输出csv文件路径
input_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
output_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_US_2024-07-16.csv'

# 读取CSV文件
df = pd.read_csv(input_csv_path)

# 筛选表现优良的广告活动
good_campaigns = df[(df['ACOS7d'] < 0.24) & 
                    (df['ACOSYesterday'] < 0.24) & 
                    (df['costYesterday'] > 0.8 * df['campaignBudget'])]

# 对符合条件广告活动增加预算，直到预算达到50
def adjust_budget(budget):
    new_budget = budget * 1.2
    return new_budget if new_budget <= 50 else 50

good_campaigns['New Budget'] = good_campaigns['campaignBudget'].apply(adjust_budget)

# 添加增加预算的原因列
good_campaigns['Reason'] = 'ACOS7d < 0.24, ACOSYesterday < 0.24, CostYesterday > 80% of Budget'

# 输出需要的列到新的CSV文件
columns_to_output = ['campaignId', 'campaignName', 'campaignBudget', 'New Budget', 'costYesterday', 
                     'clicksYesterday', 'ACOSYesterday', 'ACOS7d', 'ACOS30d', 'totalClicks30d', 
                     'totalSales30d', 'Reason']
good_campaigns[columns_to_output].to_csv(output_csv_path, index=False)

print('任务已完成并已保存结果到:', output_csv_path)