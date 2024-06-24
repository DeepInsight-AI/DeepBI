# filename: keyword_evaluation.py
import pandas as pd
import numpy as np
from datetime import date

# 读取文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'

# 读取CSV文件
df = pd.read_csv(file_path)

# 计算广告组(adGroupName)的最近7天总花费
group_cost_7d = df.groupby('adGroupName')['total_cost_7d'].sum().reset_index()
group_cost_7d.columns = ['adGroupName', 'adGroup_total_cost_7d']

# 合并广告组的总花费到原数据集
df = df.merge(group_cost_7d, on='adGroupName', how='left')

# 创建一个新的DataFrame来保存结果
result_df = pd.DataFrame(columns=[
    'date', 'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'targeting',
    'cost', 'clicks', 'total_cost_7d', 'total_sales_7d', 'total_sales_30d', 'adGroup_total_cost_7d',
    'avg_ACOS_7d', 'avg_ACOS_30d', 'new_keywordBid', 'action_reason'
])

# 定义今日日期
today = date.today()

# 规则判断与操作
for idx, row in df.iterrows():
    keyword = row['keyword']
    keywordId = row['keywordId']
    avg_ACOS_7d = row['ACOS_7d']
    avg_ACOS_30d = row['ACOS_30d']
    keywordBid = row['keywordBid']
    clicks_7d = row['total_clicks_7d']
    sales_7d = row['total_sales14d_7d']
    sales_30d = row['total_sales14d_30d']
    cost_7d = row['total_cost_7d']
    adGroup_total_cost_7d = row['adGroup_total_cost_7d']

    reason = ""
    new_keywordBid = keywordBid
    
    # 定义一
    if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "降价操作，满足定义一"
    
    # 定义二
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36:
        new_keywordBid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)
        reason = "降价操作，满足定义二"
    
    # 定义三
    elif clicks_7d >= 10 and sales_7d == 0 and avg_ACOS_30d <= 0.36:
        new_keywordBid = keywordBid - 0.04
        reason = "降价操作，满足定义三"
    
    # 定义四
    elif clicks_7d >= 10 and sales_7d == 0 and avg_ACOS_30d > 0.5:
        reason = "关闭该词，满足定义四"
        new_keywordBid = "关闭"
    
    # 定义五
    elif avg_ACOS_7d > 0.5 and avg_ACOS_30d > 0.36:
        reason = "关闭该词，满足定义五"
        new_keywordBid = "关闭"
    
    # 定义六
    elif sales_30d == 0 and cost_7d > adGroup_total_cost_7d / 5:
        reason = "关闭该词，满足定义六"
        new_keywordBid = "关闭"
    
    # 定义七
    elif sales_30d == 0 and row['total_clicks_30d'] >= 15:
        reason = "关闭该词，满足定义七"
        new_keywordBid = "关闭"
    
    # 如果有操作，则记录结果
    if reason:
        result_df = result_df.append({
            'date': today,
            'keyword': row['keyword'],
            'keywordId': row['keywordId'],
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'matchType': row['matchType'],
            'keywordBid': row['keywordBid'],
            'targeting': row['targeting'],
            'cost': row['total_cost_yesterday'],
            'clicks': row['total_clicks_yesterday'],
            'total_cost_7d': row['total_cost_7d'],
            'total_sales_7d': row['total_sales14d_7d'],
            'total_sales_30d': row['total_sales14d_30d'],
            'adGroup_total_cost_7d': adGroup_total_cost_7d,
            'avg_ACOS_7d': row['ACOS_7d'],
            'avg_ACOS_30d': row['ACOS_30d'],
            'new_keywordBid': new_keywordBid,
            'action_reason': reason
        }, ignore_index=True)

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_劣质关键词_ES_2024-06-07.csv'
result_df.to_csv(output_file_path, index=False)
print(f"结果已保存至 {output_file_path}")