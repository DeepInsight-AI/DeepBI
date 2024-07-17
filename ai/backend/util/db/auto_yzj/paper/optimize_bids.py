# filename: optimize_bids.py
import pandas as pd

# 读取CSV文件
csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(csv_path, encoding='utf-8')

# 定义筛选条件和提价
def get_bid_increase_and_reason(row):
    if (0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and
            row['ORDER_1m'] >= 2 and 0 < row['ACOS_3d'] <= 0.2):
        return 0.05, "符合定义一"
    elif (0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and
          row['ORDER_1m'] >= 2 and 0 < row['ACOS_3d'] <= 0.2):
        return 0.03, "符合定义二"
    elif (0.1 < row['ACOS_7d'] <= 0.2 and row['ACOS_30d'] <= 0.1 and
          row['ORDER_1m'] >= 2 and 0 < row['ACOS_3d'] <= 0.2):
        return 0.04, "符合定义三"
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and
          row['ORDER_1m'] >= 2 and 0 < row['ACOS_3d'] <= 0.2):
        return 0.02, "符合定义四"
    elif (0.2 < row['ACOS_7d'] <= 0.24 and row['ACOS_30d'] <= 0.1 and
          row['ORDER_1m'] >= 2 and 0 < row['ACOS_3d'] <= 0.2):
        return 0.02, "符合定义五"
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and
          row['ORDER_1m'] >= 2 and 0 < row['ACOS_3d'] <= 0.2):
        return 0.01, "符合定义六"
    else:
        return 0, ""

# 筛选符合条件的行
df['bid_increase'], df['reason'] = zip(*df.apply(get_bid_increase_and_reason, axis=1))

# 筛选出表现较好的商品投放
good_performers = df[df['bid_increase'] > 0].copy()

# 调整竞价
good_performers['New_keywordBid'] = good_performers['keywordBid'] + good_performers['bid_increase']

# 选择输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_7d', 'total_clicks_7d',
    'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'bid_increase', 'reason'
]
output_df = good_performers[output_columns]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_US_2024-07-14.csv'
output_df.to_csv(output_path, index=False, encoding='utf-8')

print("优化后的商品投放信息已保存到:", output_path)