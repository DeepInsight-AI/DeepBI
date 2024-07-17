# filename: optimize_campaign_budget.py

import pandas as pd

# 读取数据
df = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv')

# 筛选符合条件的广告活动
condition = (
    (df['ACOS7d'] < 0.24) & 
    (df['ACOSYesterday'] < 0.24) & 
    (df['costYesterday'] > (0.8 * df['campaignBudget']))
)

# 建立结果数据框架
result_df = df[condition].copy()
result_df['New Budget'] = result_df['campaignBudget'] * 1.2
result_df['New Budget'] = result_df['New Budget'].apply(lambda x: 50 if x > 50 else round(x, 2))

# 添加备注列
result_df['Remark'] = 'Increase budget due to good campaign performance'

# 筛选需要的列
needed_columns = [
    'campaignId', 
    'campaignName', 
    'campaignBudget', 
    'New Budget', 
    'costYesterday', 
    'clicksYesterday', 
    'ACOSYesterday',
    'ACOS7d', 
    'ACOS30d',
    'totalClicks30d', 
    'totalSales30d', 
    'Remark'
]

output_df = result_df[needed_columns]

# 保存结果到CSV文件
output_df.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_FR_2024-07-15.csv', index=False)

print("CSV file has been saved.")