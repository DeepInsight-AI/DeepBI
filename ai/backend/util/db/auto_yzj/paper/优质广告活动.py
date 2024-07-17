# filename: 优质广告活动.py

import pandas as pd

# 1. 读取数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\预处理.csv"
data = pd.read_csv(file_path)

# 2. 业务逻辑和数据筛选
filtered_data = data[
    (data['ACOS7d'] < 0.24) & 
    (data['ACOSYesterday'] < 0.24) & 
    (data['costYesterday'] > 0.8 * data['campaignBudget'])
]

# 增加预算
def adjust_budget(row):
    # 增加0.2倍预算，直到预算为50
    new_budget = row['campaignBudget']
    while new_budget * 1.2 <= 50:
        new_budget *= 1.2
    return new_budget

filtered_data['New Budget'] = filtered_data.apply(adjust_budget, axis=1)

# 3. 选取所需字段并添加原因
filtered_data['理由'] = '最近7天ACOS低于0.24, 昨天的ACOS也低于0.24且花费超过了80%预算'

output_data = filtered_data[[
    'campaignId', 'campaignName', 'campaignBudget', 'New Budget', 'costYesterday', 
    'clicksYesterday', 'ACOSYesterday', 'ACOS7d', 'countryAvgACOS1m', 'totalClicks30d', 'totalSales30d', '理由'
]]

# 4. 输出结果到CSV文件
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\提问策略\\SD_优质sd广告活动_v1_1_LAPASA_ES_2024-07-15.csv"
output_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print("成功生成CSV文件，路径为:", output_path)