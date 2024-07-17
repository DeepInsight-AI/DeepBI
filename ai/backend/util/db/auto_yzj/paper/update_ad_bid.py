# filename: update_ad_bid.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
df = pd.read_csv(file_path)

# 识别表现差的广告组及商品
poor_ad_groups = df[df['total_sales_15d'] == 0]['adGroupName'].unique()
poor_ads = df[(df['adGroupName'].isin(poor_ad_groups)) & (df['total_clicks_7d'] <= 12)].copy()

# 提高竞价0.02
poor_ads['new_keywordBid'] = poor_ads['keywordBid'] + 0.02
poor_ads['调整原因'] = '广告组的总销售额为0且点击次数小于等于12'

# 选择需要的列
output_columns = [
    'campaignName', 'adGroupName', 'total_sales_15d',
    'total_clicks_7d', 'keyword', 'matchType', 'keywordBid', 
    'keywordId', 'new_keywordBid', '调整原因'
]
poor_ads_output = poor_ads[output_columns]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_FR_2024-07-02.csv'
poor_ads_output.to_csv(output_file_path, index=False)

print(f"处理完毕，结果保存在: {output_file_path}")