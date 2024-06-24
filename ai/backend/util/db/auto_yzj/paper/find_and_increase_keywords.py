# filename: find_and_increase_keywords.py

import pandas as pd
from datetime import datetime

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv"
df = pd.read_csv(file_path)

# 数据过滤与计算
filtered_keywords = []

for index, row in df.iterrows():
    increase_amount = 0
    reason = ""

    # 定义一
    if (0 < row['ACOS_7d'] <= 0.1 and 
        0 < row['ACOS_30d'] <= 0.1 and 
        row['ORDER_1m'] >= 2):
        increase_amount = 0.05
        reason = "定义一"

    # 定义二
    elif (0 < row['ACOS_7d'] <= 0.1 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2):
        increase_amount = 0.03
        reason = "定义二"

    # 定义三
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 
          0 < row['ACOS_30d'] <= 0.1 and 
          row['ORDER_1m'] >= 2):
        increase_amount = 0.04
        reason = "定义三"

    # 定义四
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2):
        increase_amount = 0.02
        reason = "定义四"

    # 定义五
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 
          0 < row['ACOS_30d'] <= 0.1 and 
          row['ORDER_1m'] >= 2):
        increase_amount = 0.02
        reason = "定义五"

    # 定义六
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2):
        increase_amount = 0.01
        reason = "定义六"
    
    if increase_amount > 0:
        filtered_keywords.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "keyword": row['keyword'],
            "keywordId": row['keywordId'],
            "campaignName": row['campaignName'],
            "adGroupName": row['adGroupName'],
            "matchType": row['matchType'],
            "keywordBid": row['keywordBid'],
            "targeting": row['targeting'],
            "cost": row['total_cost_7d'],
            "clicks": row['total_clicks_7d'],
            "recent_ACOS_7d": row['ACOS_7d'],
            "recent_ACOS_30d": row['ACOS_30d'],
            "recent_orders_1m": row['ORDER_1m'],
            "increase_amount": increase_amount,
            "reason": reason
        })

# 将结果保存到CSV文件中
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\优质关键词_FR.csv"
output_df = pd.DataFrame(filtered_keywords)
output_df.to_csv(output_file_path, index=False)

# 输出提示信息
print(f"数据已保存至 {output_file_path}")