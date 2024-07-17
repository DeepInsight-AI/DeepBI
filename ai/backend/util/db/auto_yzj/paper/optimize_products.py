# filename: optimize_products.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 初始化结果列表
result = []

# 定义对表现较差的商品投放的识别和操作
for _, row in df.iterrows():
    keyword = row['keyword']
    keywordId = row['keywordId']
    campaignName = row['campaignName']
    adGroupName = row['adGroupName']
    matchType = row['matchType']
    keywordBid = row['keywordBid']
    targeting = row['targeting']
    total_cost_7d = row['total_cost_7d']
    total_sales14d_7d = row['total_sales14d_7d']
    total_cost_30d = row['total_cost_30d']
    total_clicks_30d = row['total_clicks_30d']
    total_clicks_7d = row['total_clicks_7d']
    total_sales14d_30d = row['total_sales14d_30d']
    ACOS_7d = row['ACOS_7d']
    ACOS_30d = row['ACOS_30d']
    new_keywordBid = keywordBid
    
    action_reason = None
    
    if 0.24 < ACOS_7d <= 0.5 and 0 < ACOS_30d <= 0.5:
        new_keywordBid = keywordBid / ((ACOS_7d - 0.24) / 0.24 + 1)
        action_reason = "调整竞价定义一"
    
    elif ACOS_7d > 0.5 and ACOS_30d <= 0.36:
        new_keywordBid = keywordBid / ((ACOS_7d - 0.24) / 0.24 + 1)
        action_reason = "调整竞价定义二"
    
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and ACOS_30d <= 0.36:
        new_keywordBid -= 0.04
        action_reason = "调整竞价定义三"
    
    elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and ACOS_30d > 0.5:
        new_keywordBid = "关闭"
        action_reason = "关闭定义四"
    
    elif ACOS_7d > 0.5 and ACOS_30d > 0.36:
        new_keywordBid = "关闭"
        action_reason = "关闭定义五"
    
    elif total_sales14d_30d == 0 and total_cost_30d >= 5:
        new_keywordBid = "关闭"
        action_reason = "关闭定义六"
    
    elif total_sales14d_30d == 0 and total_clicks_30d >= 15 and total_clicks_7d > 0:
        new_keywordBid = "关闭"
        action_reason = "关闭定义七"
    
    if action_reason:
        result.append({
            "keyword": keyword,
            "keywordId": keywordId,
            "campaignName": campaignName,
            "adGroupName": adGroupName,
            "matchType": matchType,
            "keywordBid": keywordBid,
            "new_keywordBid": new_keywordBid,
            "targeting": targeting,
            "total_cost_7d": total_cost_7d,
            "total_sales14d_7d": total_sales14d_7d,
            "total_cost_30d": total_cost_30d,
            "total_clicks_30d": total_clicks_30d,
            "total_clicks_7d": total_clicks_7d,
            "total_sales14d_30d": total_sales14d_30d,
            "ACOS_7d": ACOS_7d,
            "ACOS_30d": ACOS_30d,
            "action_reason": action_reason
        })

# 保存结果到新CSV文件
result_df = pd.DataFrame(result)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_IT_2024-07-09.csv'
result_df.to_csv(output_path, index=False)

print(f"结果已保存到: {output_path}")