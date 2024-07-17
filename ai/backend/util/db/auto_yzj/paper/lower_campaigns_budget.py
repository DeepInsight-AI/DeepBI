# filename: lower_campaigns_budget.py
import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义判定条件
def filter_poor_campaigns(data):
    # 定义一
    condition1 = (data['ACOS7d'] > 0.24) & (data['ACOSYesterday'] > 0.24) & \
                 (data['costYesterday'] > 5.5) & (data['ACOS30d'] > data['countryAvgACOS1m'])
    
    # 定义二
    condition2 = (data['ACOS30d'] > 0.24) & (data['ACOS30d'] > data['countryAvgACOS1m']) & \
                 (data['totalSales7d'] == 0) & (data['totalCost7d'] > 10)
    
    # 筛选出符合条件的广告活动
    poor_campaigns = data[condition1 | condition2].copy()
    
    return poor_campaigns, condition1, condition2

# 处理预算
def adjust_budget(poor_campaigns, condition1, condition2):
    poor_campaigns['New Budget'] = poor_campaigns['campaignBudget']
    reasons = []

    for index, row in poor_campaigns.iterrows():
        new_budget = row['campaignBudget']
        reason = []

        # 定义一的处理
        if condition1.loc[index]:
            reason.append('定义一')
            if row['campaignBudget'] > 13:
                new_budget = max(row['campaignBudget'] - 5, 8)

        # 定义二的处理
        if condition2.loc[index]:
            reason.append('定义二')
            if row['campaignBudget'] > 10:
                new_budget = max(row['campaignBudget'] - 5, 5)

        # 更新预算
        poor_campaigns.at[index, 'New Budget'] = new_budget
        reasons.append(';'.join(reason) if reason else '未知原因')

    poor_campaigns['Reason'] = reasons
    return poor_campaigns

# 保存结果到指定的csv文件
def save_results(poor_campaigns, output_file):
    output_columns = [
        'campaignId', 'campaignName', 'campaignBudget', 'New Budget', 'clicksYesterday', 'ACOSYesterday',
        'ACOS7d', 'totalSales7d', 'totalCost7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d', 'countryAvgACOS1m', 'Reason'
    ]
    poor_campaigns = poor_campaigns[output_columns]
    poor_campaigns.to_csv(output_file, index=False)

# 主函数
def main():
    poor_campaigns, condition1, condition2 = filter_poor_campaigns(data)
    adjusted_campaigns = adjust_budget(poor_campaigns, condition1, condition2)
    output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_FR_2024-07-11.csv'
    save_results(adjusted_campaigns, output_file)
    print(f"输出结果保存至 {output_file}")

if __name__ == "__main__":
    main()