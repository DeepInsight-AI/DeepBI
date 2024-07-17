# filename: optimize_ad_campaigns.py

import pandas as pd

try:
    # Step 1: 读取CSV数据
    file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv"
    data = pd.read_csv(file_path)

    # Step 2: 筛选符合条件的广告活动
    filtered_data = data[
        (data['ACOS7d'] < 0.24) & 
        (data['ACOSYesterday'] < 0.24) & 
        (data['costYesterday'] > data['campaignBudget'] * 0.8)
    ]

    # Step 3: 增加预算计算
    def adjust_budget(budget):
        new_budget = budget * 1.2
        return new_budget if new_budget <= 50 else 50

    filtered_data['NewBudget'] = filtered_data['campaignBudget'].apply(adjust_budget)

    # 增加一个说明列
    filtered_data['Reason'] = 'High performance with low ACOS and high expenditure'

    # Step 4: 导出结果
    output_columns = [
        'campaignId', 'campaignName', 'campaignBudget', 'NewBudget',
        'costYesterday', 'clicksYesterday', 'ACOSYesterday', 
        'ACOS7d', 'countryAvgACOS1m', 'totalClicks30d', 'totalSales30d', 'Reason'
    ]
    output_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_优质sd广告活动_v1_1_LAPASA_DE_2024-07-16.csv"
    filtered_data.to_csv(output_file, columns=output_columns, index=False)

    print(f"Results saved to {output_file}")

except Exception as e:
    print(f"An error occurred: {e}")