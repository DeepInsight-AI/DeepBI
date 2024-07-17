# filename: process_ad_campaigns.py
import pandas as pd

# 加载CSV数据
csv_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\预处理.csv"
df = pd.read_csv(csv_file_path)

# 筛选符合条件的广告活动
good_campaigns = df[
    (df['ACOS7d'] < 0.24)
    & (df['ACOSYesterday'] < 0.24)
    & (df['costYesterday'] > 0.8 * df['campaignBudget'])
]

# 调整预算，确保调整后的预算最大不超过50
good_campaigns['New Budget'] = good_campaigns['campaignBudget'] * 1.2
good_campaigns['New Budget'] = good_campaigns['New Budget'].apply(lambda x: min(x, 50))

# 增加字段"原因"
good_campaigns['原因'] = 'ACOS合适，花费大于80%预算'

# 需要的字段
output_fields = [
    "campaignId", "campaignName", "campaignBudget", "New Budget", "costYesterday",
    "clicksYesterday", "salesYesterday", "ACOSYesterday", "ACOS7d",
    "ACOS30d", "totalClicks30d", "totalSales30d", "原因"
]

output_df = good_campaigns[output_fields]

# 输出结果到CSV文件
output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\sd广告\\预算优化\\提问策略\\SD_优质sd广告活动_v1_1_LAPASA_UK_2024-07-16.csv"
output_df.to_csv(output_file_path, index=False)

print(f"处理完成，结果已保存到文件：{output_file_path}")