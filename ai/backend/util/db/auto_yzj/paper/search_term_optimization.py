# filename: search_term_optimization.py
import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv"
df = pd.read_csv(file_path)

# 定义原因
def find_reason(row):
    reasons = []
    if row['ACOS_30d'] > 0.5 and row['total_clicks_30d'] > 10 and row['total_sales14d_30d'] < 0.1 * row['total_cost_30d']:
        reasons.append("定义一：高ACOS但点击多，销售额少")
    if row['ACOS_30d'] > 0.5 and row['total_sales14d_30d'] < 0.1 * row['total_cost_30d']:
        reasons.append("定义二：极高ACOS，销售额少")
    if row['total_clicks_30d'] > 10 and row['total_cost_30d'] > 0 and row['total_sales14d_30d'] == 0:
        reasons.append("定义三：点击多，有花费，但没销售额")
    return "; ".join(reasons) if reasons else None

# 筛选符合条件的搜索词
df['reason'] = df.apply(find_reason, axis=1)
filtered_df = df[df['reason'].notnull()]

# 选择需要输出的列
output_df = filtered_df[['campaignName', 'campaignId', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'matchType', 'reason']]

# 保存结果到CSV
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_劣质搜索词_v1_1_ES_2024-06-12.csv"
output_df.to_csv(output_path, index=False)

print("结果已保存到", output_path)