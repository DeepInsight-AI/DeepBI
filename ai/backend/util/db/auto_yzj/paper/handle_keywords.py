# filename: handle_keywords.py
import pandas as pd

# 读取CSV文件
df = pd.read_csv(r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv")

# 过滤广告组的最近15天的总销售额为0的广告组
zero_sales_df = df[df['total_sales_15d'] == 0]

# 找出符合条件的广告组
ad_groups = zero_sales_df.groupby('adGroupName').filter(lambda x: (x['total_clicks_7d'] <= 12).all())

# 对符合条件的关键词竞价增加0.02
ad_groups['adjusted_keywordBid'] = ad_groups['keywordBid'] + 0.02
ad_groups['reason'] = '广告组的最近15天的总销售额为0，并且广告组里的所有关键词的最近7天的总点击次数小于12'

# 选择要输出的列
output_df = ad_groups[[
    'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d',
    'keyword', 'matchType', 'keywordBid', 'keywordId', 'adjusted_keywordBid', 'reason'
]]

# 输出到新的CSV文件
output_df.to_csv(r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_IT_2024-06-08.csv", index=False)

print("Process completed! The results are saved in the specified CSV file.")