# filename: find_poor_performance_campaigns.py
import pandas as pd

# 读取数据集
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv"

try:
    data = pd.read_csv(file_path)
    print("数据集读取成功")
except Exception as e:
    print(f"数据集读取失败: {e}")
    raise

# 确认数据列名
print("数据集中包含的列名如下：")
print(data.columns)

# 定义分析用的今日日期
today_date = pd.to_datetime("2024-05-28")

# 添加昨天的日期
data['date'] = pd.to_datetime(data['date'])
yesterday_date = today_date - pd.Timedelta(days=1)
print(f"昨日日期: {yesterday_date}")

# 初步筛选判断
print("进行初步筛选...")

poor_campaigns = []
for idx, row in data.iterrows():
    if row['date'] == yesterday_date:
        condition_1 = (row['avg_ACOS_7d'] > 0.24) and (row['ACOS'] > 0.24) and (row['clicks'] >= 10) and (row['avg_ACOS_1m'] > row['country_avg_ACOS_1m'])
        condition_2 = (row['avg_ACOS_7d'] > 0.24) and (row['ACOS'] > 0.24) and (row['cost'] > 0.8 * row['Budget']) and (row['avg_ACOS_1m'] > row['country_avg_ACOS_1m'])
        condition_3 = (row['avg_ACOS_1m'] > 0.24) and (row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']) and (row['sales_7d'] == 0) and (row['clicks_7d'] >= 15)

        if condition_1 or condition_2 or condition_3:
            if condition_1:
                reason = "定义一"
                new_budget = max(8, row['Budget'] - 5)
            elif condition_2:
                reason = "定义二"
                new_budget = max(8, row['Budget'] - 5)
            elif condition_3:
                reason = "定义三"
                new_budget = max(5, row['Budget'] - 5)

            poor_campaigns.append({
                'date': row['date'],
                'campaignName': row['campaignName'],
                'Budget': row['Budget'],
                'clicks': row['clicks'],
                'ACOS': row['ACOS'],
                'avg_ACOS_7d': row['avg_ACOS_7d'],
                'clicks_7d': row['clicks_7d'],
                'sales_7d': row['sales'],
                'avg_ACOS_1m': row['avg_ACOS_1m'],
                'clicks_1m': row['clicks_1m'],
                'sales_1m': row['sales_1m'],
                'country_avg_ACOS_1m': row['country_avg_ACOS_1m'],
                'new_budget': new_budget,
                'reason': reason
            })

print(f"筛选出 {len(poor_campaigns)} 个劣质广告活动")

if poor_campaigns:
    output_df = pd.DataFrame(poor_campaigns)
    output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_劣质广告活动_IT_2024-06-11.csv"
    output_df.to_csv(output_file_path, index=False)
    print(f"结果已经保存到 {output_file_path}")
else:
    print("未筛选出符合条件的广告活动")