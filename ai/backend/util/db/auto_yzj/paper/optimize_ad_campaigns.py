# filename: optimize_ad_campaigns.py
import pandas as pd

# 原始数据文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'

# 尝试使用不同编码读取
data = pd.read_csv(file_path, encoding='utf-8-sig')

# 创建一个存储结果的列表
results = []

# 定义过滤条件
def is_poor_performing(row):
    # 定义一
    if (row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and
        row['total_clicks_7d'] >= 10 and row['ACOS_30d'] > row['country_avg_ACOS_1m']):
        return '定义一'

    # 定义二
    if (row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and
        row['total_cost_7d'] > 0.8 * (row['total_cost_30d'] / 30) and
        row['ACOS_30d'] > row['country_avg_ACOS_1m']):
        return '定义二'

    # 定义三
    if (row['ACOS_30d'] > 0.24 and row['ACOS_30d'] > row['country_avg_ACOS_1m'] and
        row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] >= 15):
        return '定义三'

    # 定义四
    if (row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 75):
        return '定义四'

    return None

# 应用条件
for idx, row in data.iterrows():
    reason = is_poor_performing(row)
    if reason:
        new_budget = max(8, row['total_cost_30d'] - 5)
        if reason == '定义四':
            new_budget = '关闭'

        result = {
            "campaignId": row['campaignId'],
            "campaignName": row['campaignName'],
            "Budget": row['total_cost_30d'],
            "New Budget": new_budget,
            "clicks": row['total_clicks_7d'],
            "ACOS": row['ACOS_yesterday'],
            "ACOS_7d": row['ACOS_7d'],
            "total_clicks_7d": row['total_clicks_7d'],
            "total_sales14d_7d": row['total_sales14d_7d'],
            "ACOS_30d": row['ACOS_30d'],
            "total_clicks_30d": row['total_clicks_30d'],
            "total_sales14d_30d": row['total_sales14d_30d'],
            "country_avg_ACOS_1m": row['country_avg_ACOS_1m'],
            "Reason": reason
        }
        results.append(result)

# 转换为DataFrame并保存结果到CSV
results_df = pd.DataFrame(results)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_v1_1_ES_2024-06-121.csv'
results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f'File saved successfully to {output_file_path} with {results_df.shape[0]} rows.')
print(results_df.head())