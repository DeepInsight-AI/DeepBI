# filename: optimize_keyword_bids.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义新的数据框来存储满足条件的自动定位词和降价原因
result = []

# 逻辑判断
for index, row in data.iterrows():
    keyword = row['keyword']
    ACOS_30d = row['ACOS_30d']
    ACOS_7d = row['ACOS_7d']
    total_sales14d_7d = row['total_sales14d_7d']
    total_clicks_7d = row['total_clicks_7d']
    
    # 定义一
    if 0.24 < ACOS_7d < 0.5 and 0 < ACOS_30d < 0.24:
        result.append({
            "campaignName": row["campaignName"],
            "adGroupName": row["adGroupName"],
            "keyword": keyword,
            "ACOS_30d": ACOS_30d,
            "ACOS_7d": ACOS_7d,
            "Reason": "Definition 1: Lower bid by 0.03"
        })
    
    # 定义二
    elif 0.24 < ACOS_7d < 0.5 and 0.24 < ACOS_30d < 0.5:
        result.append({
            "campaignName": row["campaignName"],
            "adGroupName": row["adGroupName"],
            "keyword": keyword,
            "ACOS_30d": ACOS_30d,
            "ACOS_7d": ACOS_7d,
            "Reason": "Definition 2: Lower bid by 0.04"
        })
    
    # 定义三
    elif total_sales14d_7d == 0 and total_clicks_7d > 0 and 0.24 < ACOS_30d < 0.5:
        result.append({
            "campaignName": row["campaignName"],
            "adGroupName": row["adGroupName"],
            "keyword": keyword,
            "ACOS_30d": ACOS_30d,
            "ACOS_7d": ACOS_7d,
            "Reason": "Definition 3: Lower bid by 0.04"
        })
    
    # 定义四
    elif 0.24 < ACOS_7d < 0.5 and ACOS_30d > 0.5:
        result.append({
            "campaignName": row["campaignName"],
            "adGroupName": row["adGroupName"],
            "keyword": keyword,
            "ACOS_30d": ACOS_30d,
            "ACOS_7d": ACOS_7d,
            "Reason": "Definition 4: Lower bid by 0.05"
        })
    
    # 定义五
    elif ACOS_7d > 0.5 and 0 < ACOS_30d < 0.24:
        result.append({
            "campaignName": row["campaignName"],
            "adGroupName": row["adGroupName"],
            "keyword": keyword,
            "ACOS_30d": ACOS_30d,
            "ACOS_7d": ACOS_7d,
            "Reason": "Definition 5: Lower bid by 0.05"
        })

# 将结果数据框转换为CSV文件
result_df = pd.DataFrame(result)
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\劣质自动定位组_ES_2024-6-02.csv'
result_df.to_csv(output_file, index=False)

print("Processing complete. Results saved to", output_file)