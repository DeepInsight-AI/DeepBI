# filename: script_process_special_keywords.py
import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv"
df = pd.read_csv(file_path)

# 筛选出满足广告组总销售额为0，点击次数小于等于12的记录
filtered_df = df[(df['total_sales_15d'] == 0) & (df['total_clicks_7d'] <= 12)]

# 提高关键词竞价0.02
filtered_df['New Bid'] = filtered_df['keywordBid'] + 0.02
filtered_df['操作原因'] = '广告组最近15天的总销售额为0，且广告组里的所有关键词的最近7天的总点击次数小于等于12'

# 选择所需的列并重命名
output_df = filtered_df[['campaignName', 'adGroupName', 'total_sales_15d', 
                         'total_clicks_7d', 'keyword', 'matchType', 
                         'keywordBid', 'keywordId', 'New Bid', '操作原因']]

# 保存结果到新的CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_v1_1_ES_2024-06-14.csv"
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("关键词分析已完成，结果保存在：", output_file_path)