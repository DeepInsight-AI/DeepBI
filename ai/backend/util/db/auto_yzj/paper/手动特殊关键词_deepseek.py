# filename: adjust_keyword_bids.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv')

# 筛选满足条件的关键词
filtered_keywords = data[(data['total_sales_15d'] == 0) & (data['total_clicks_7d'] <= 12)]

# 对满足条件的关键词提高竞价0.02
filtered_keywords['keywordBid'] = filtered_keywords['keywordBid'] + 0.02
filtered_keywords['New Bid'] = filtered_keywords['keywordBid']
filtered_keywords['操作竞价原因'] = '满足定义：广告组的最近15天的总销售额为0，并且广告组里的所有关键词的最近7天的总点击次数小于等于12'

# 输出筛选出的关键词及其相关信息到新的CSV文件
filtered_keywords[['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 'matchType', 'keywordBid', 'keywordId', 'New Bid', '操作竞价原因']].to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_v1_1_IT_2024-06-13_deepseek.csv', index=False)