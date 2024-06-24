# filename: process_automated_targeting.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 初始化结果列表
results = []

# 定义降价原因的条件
for index, row in data.iterrows():
    keyword = row['keyword']
    keyword_bid = row['keywordBid']
    ad_group_name = row['adGroupName']
    campaign_name = row['campaignName']
    acos_30d = row['ACOS_30d']
    acos_7d = row['ACOS_7d']
    total_sales_7d = row['total_sales14d_7d']
    total_clicks_7d = row['total_clicks_7d']
    
    discount_reason = ""
    if 0 < acos_30d < 0.24 and 0.24 < acos_7d < 0.5:
        discount_reason = "定义一：降价0.03"
    elif 0.24 < acos_30d < 0.5 and 0.24 < acos_7d < 0.5:
        discount_reason = "定义二：降价0.04"
    elif total_sales_7d == 0 and total_clicks_7d > 0 and 0.24 < acos_30d < 0.5:
        discount_reason = "定义三：降价0.04"
    elif acos_30d > 0.5 and 0.24 < acos_7d < 0.5:
        discount_reason = "定义四：降价0.05"
    elif 0 < acos_30d < 0.24 and acos_7d > 0.5:
        discount_reason = "定义五：降价0.05"
    
    if discount_reason:
        results.append({
            'campaignName': campaign_name,
            'adGroupName': ad_group_name,
            'keyword': keyword,
            'ACOS_30d': acos_30d,
            'ACOS_7d': acos_7d,
            '降价的原因': discount_reason
        })

# 将结果保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_IT_2024-06-08.csv'
results_df = pd.DataFrame(results)
results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f'保存成功，结果文件路径为: {output_file_path}')