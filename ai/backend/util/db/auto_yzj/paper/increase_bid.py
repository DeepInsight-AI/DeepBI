# filename: increase_bid.py
import pandas as pd

# 1. 读取CSV数据
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_IT_2024-06-30.csv'

df = pd.read_csv(input_file)

# 2. 过滤数据：查找总销售额为0的广告组
ad_groups_with_zero_sales = df[df['total_sales_15d'] == 0]['adGroupName'].unique()

# 3. 进一步过滤：找出广告组里的所有商品投放的点击次数 <= 12 的广告组
filtered_ad_groups = []
for ad_group in ad_groups_with_zero_sales:
    ad_group_data = df[df['adGroupName'] == ad_group]
    if all(ad_group_data['total_clicks_7d'] <= 12):
        filtered_ad_groups.append(ad_group)

# 4. 调整竞价并准备输出数据
result_data = []
for ad_group in filtered_ad_groups:
    ad_group_data = df[df['adGroupName'] == ad_group]
    ad_group_data['New Bid'] = ad_group_data['keywordBid'] + 0.02
    ad_group_data['Reason'] = 'Increase bid due to low sales and low clicks'
    result_data.append(ad_group_data)

# 合并所有结果数据
if result_data:
    final_result = pd.concat(result_data)
    final_result.to_csv(output_file, index=False)
else:
    print("No matching data found.")

print(f"The result has been saved to {output_file}.")