# filename: filter_and_update_ad_campaigns.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选劣质广告活动
def filter_campaigns(df):
    results = []

    for index, row in df.iterrows():
        new_budget = row['campaignBudget']
        reason = ""

        # 定义一的条件
        if (row['ACOS7d'] > 0.24) and (row['ACOSYesterday'] > 0.24) and (row['costYesterday'] > 5.5) and (row['ACOS30d'] > row['countryAvgACOS1m']):
            if row['campaignBudget'] > 13:
                new_budget = max(8, row['campaignBudget'] - 5)
            elif row['campaignBudget'] < 8:
                new_budget = row['campaignBudget']
            
            reason = "满足定义一：降低预算"

        # 定义二的条件
        elif (row['ACOS30d'] > 0.24) and (row['ACOS30d'] > row['countryAvgACOS1m']) and (row['totalSales7d'] == 0) and (row['totalCost7d'] > 10):
            new_budget = max(5, row['campaignBudget'] - 5)
            reason = "满足定义二：降低预算"

        if reason:
            results.append({
                "campaignId": row['campaignId'],
                "campaignName": row['campaignName'],
                "Budget": row['campaignBudget'],
                "New Budget": new_budget,
                "clicksYesterday": row['clicksYesterday'],
                "ACOS": row['ACOSYesterday'],
                "最近7天的平均ACOS值": row['ACOS7d'],
                "最近7天的总点击次数": row['totalClicks7d'],
                "最近7天的总销售": row['totalSales7d'],
                "最近一个月的平均ACOS值": row['ACOS30d'],
                "最近一个月的总点击数": row['totalClicks30d'],
                "最近一个月的总销售": row['totalSales30d'],
                "该国家广告活动最近一个月整体的平均ACOS值": row['countryAvgACOS1m'],
                "降低预算原因": reason
            })

    return results

# 获取劣质广告活动
filtered_campaigns = filter_campaigns(data)

# 转换为DataFrame
filtered_df = pd.DataFrame(filtered_campaigns)

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_US_2024-07-14.csv'
filtered_df.to_csv(output_path, index=False)

print("过滤和更新后的广告活动已保存到: ", output_path)