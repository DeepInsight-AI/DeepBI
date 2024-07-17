# filename: process_underperforming_campaigns.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 确定表现较差的广告活动
def find_underperforming_campaigns(df):
    underperforming_campaigns = []

    for _, row in df.iterrows():
        new_budget = row['campaignBudget']
        reason = ""
        # 定义一
        if row['ACOS7d'] > 0.24 and row['ACOSYesterday'] > 0.24 and row['costYesterday'] > 5.5 and row['ACOS30d'] > row['countryAvgACOS1m']:
            reason = "Definition 1"
            if row['campaignBudget'] > 13:
                new_budget = max(8, row['campaignBudget'] - 5)
        # 定义二
        elif row['ACOS30d'] > 0.24 and row['ACOS30d'] > row['countryAvgACOS1m'] and row['totalSales7d'] == 0 and row['totalCost7d'] > 10:
            reason = "Definition 2"
            new_budget = max(5, row['campaignBudget'] - 5)
        
        # 如果广告活动符合任一定义，且预算发生了改变
        if reason and new_budget != row['campaignBudget']:
            underperforming_campaigns.append({
                "campaignId": row['campaignId'],
                "campaignName": row['campaignName'],
                "Budget": row['campaignBudget'],
                "New Budget": new_budget,
                "clicks": row['clicksYesterday'],
                "ACOS": row['ACOSYesterday'],
                "最近7天的平均ACOS值": row['ACOS7d'],
                "最近7天的总点击次数": row['totalClicks7d'],
                "最近7天的总销售": row['totalSales7d'],
                "最近一个月的平均ACOS值": row['ACOS30d'],
                "最近一个月的总点击数": row['totalClicks30d'],
                "最近一个月的总销售": row['totalSales30d'],
                "countryAvgACOS1m": row['countryAvgACOS1m'],
                "原因": reason
            })
    
    return underperforming_campaigns

underperforming_campaigns = find_underperforming_campaigns(df)

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_ES_2024-07-16.csv'
output_df = pd.DataFrame(underperforming_campaigns)
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("处理完毕。结果已保存到指定路径。")