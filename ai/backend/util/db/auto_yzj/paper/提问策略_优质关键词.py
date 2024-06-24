# filename: 提问策略_优质关键词.py

import pandas as pd
from datetime import datetime, timedelta

# 定义今天的日期
today = datetime(2024, 5, 28)

# 读取CSV文件
file_path = r"C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv"
data = pd.read_csv(file_path)

# 获取昨天的日期字符串
yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')

# 筛选条件
def filter_keywords(row):
    if (0 < row['avg_ACOS_7d'] <= 0.1) and (0 < row['avg_ACOS_1m'] <= 0.1) and (row['sales_1m'] >= 2):
        return 0.05, "定义一"
    elif (0 < row['avg_ACOS_7d'] <= 0.1) and (0.1 < row['avg_ACOS_1m'] <= 0.24) and (row['sales_1m'] >= 2):
        return 0.03, "定义二"
    elif (0.1 < row['avg_ACOS_7d'] <= 0.2) and (row['avg_ACOS_1m'] <= 0.1) and (row['sales_1m'] >= 2):
        return 0.04, "定义三"
    elif (0.1 < row['avg_ACOS_7d'] <= 0.2) and (0.1 < row['avg_ACOS_1m'] <= 0.24) and (row['sales_1m'] >= 2):
        return 0.02, "定义四"
    elif (0.2 < row['avg_ACOS_7d'] <= 0.24) and (row['avg_ACOS_1m'] <= 0.1) and (row['sales_1m'] >= 2):
        return 0.02, "定义五"
    elif (0.2 < row['avg_ACOS_7d'] <= 0.24) and (0.1 < row['avg_ACOS_1m'] <= 0.24) and (row['sales_1m'] >= 2):
        return 0.01, "定义六"
    else:
        return None, None

# 筛选关键词并计算新的竞价
results = []
for _, row in data.iterrows():
    increase_amount, reason = filter_keywords(row)
    if increase_amount is not None:
        results.append({
            "date": yesterday,
            "campaignName": row['campaignName'],
            "campaignId": row['campaignId'],
            "clicks": row['clicks'],
            "cost": row['cost'],
            "sales": row['sales'],
            "avg_ACOS_7d": row['avg_ACOS_7d'],
            "avg_ACOS_1m": row['avg_ACOS_1m'],
            "sales_1m": row['sales_1m'],
            "提价金额": increase_amount,
            "提价原因": reason
        })

# 创建DataFrame并导出到CSV
output_df = pd.DataFrame(results)
output_file_path = r"C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\优质关键词.csv"
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("CSV文件已成功生成:", output_file_path)