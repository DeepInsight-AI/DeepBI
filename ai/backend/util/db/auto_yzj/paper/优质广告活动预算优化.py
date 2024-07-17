# filename: 优质广告活动预算优化.py

import pandas as pd

# 读取数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\预处理.csv"
data = pd.read_csv(file_path)

# 条件筛选
qualified_campaigns = data[
    (data["ACOS7d"] < 0.24) & 
    (data["ACOSYesterday"] < 0.24) & 
    (data["costYesterday"] > 0.8 * data["campaignBudget"])
]

# 增加预算并限制到50
qualified_campaigns["new_campaignBudget"] = qualified_campaigns["campaignBudget"] * 1.2
qualified_campaigns["new_campaignBudget"] = qualified_campaigns["new_campaignBudget"].apply(lambda x: x if x <= 50 else 50)

# 输出到新的CSV文件
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\提问策略\\SD_优质sd广告活动_v1_1_LAPASA_UK_2024-07-15.csv"
qualified_campaigns[[
    "campaignId",
    "campaignName",
    "campaignBudget",
    "new_campaignBudget",
    "costYesterday",
    "clicksYesterday",
    "ACOSYesterday",
    "ACOS7d",
    "countryAvgACOS1m",
    "totalClicks30d",
    "totalSales30d"
]].to_csv(output_path, index=False, encoding='utf-8-sig')

print("已将符合条件的广告活动信息输出到文件:", output_path)