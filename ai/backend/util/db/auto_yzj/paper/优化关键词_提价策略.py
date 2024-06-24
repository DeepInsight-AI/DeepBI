# filename: 优化关键词_提价策略.py
import pandas as pd
from datetime import datetime

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_优质关键词_ES_2024-06-05.csv'

# 读取CSV文件
df = pd.read_csv(input_file, encoding='utf-8')

# 定义条件函数
def get_bid_increment(row):
    if 0 < row['ACOS_7d'] <= 0.1:
        if 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            return 0.05, "定义一"
        elif 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            return 0.03, "定义二"
    elif 0.1 < row['ACOS_7d'] <= 0.2:
        if 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            return 0.04, "定义三"
        elif 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            return 0.02, "定义四"
    elif 0.2 < row['ACOS_7d'] <= 0.24:
        if 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            return 0.02, "定义五"
        elif 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            return 0.01, "定义六"
    return 0, ""

# 获取当前日期
current_date = datetime.now().strftime('%Y-%m-%d')

# 过滤符合条件的关键词并提价
df['bid_increment'], df['reason'] = zip(*df.apply(get_bid_increment, axis=1))
filtered_df = df[df['bid_increment'] > 0]

# 筛选并保存结果
result = filtered_df[['keywordId', 'keyword', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'targeting', 
                      'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 
                      'bid_increment', 'reason']]
result['date'] = current_date

result = result[['date', 'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
                 'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 
                 'bid_increment', 'reason']]

result.to_csv(output_file, index=False, encoding='utf-8')

print(f"CSV file has been saved as {output_file}")