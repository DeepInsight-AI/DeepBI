# filename: detect_poor_search_terms.py
import pandas as pd

# 读取 CSV 数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义判断条件和函数
def determine_reason(row):
    reasons = []
    if row['ACOS_7d'] > 0.30 and row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] < row['total_cost_7d'] * 0.10:
        reasons.append("定义一")
    if row['ACOS_7d'] > 0.50 and row['total_sales14d_7d'] < row['total_cost_7d'] * 0.10:
        reasons.append("定义二")
    if row['total_clicks_30d'] > 10 and row['total_cost_30d'] > 0 and row['total_sales14d_30d'] == 0:
        reasons.append("定义三")
    return "; ".join(reasons)

# 筛选并添加原因列
data['reason'] = data.apply(determine_reason, axis=1)
filtered_data = data[data['reason'] != ""]

# 选择需要的列
output_columns = [
    'campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'total_cost_7d', 
    'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'matchType', 'reason'
]
result = filtered_data[output_columns]

# 保存结果到指定路径
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_劣质搜索词_v1_1_ES_2024-06-121.csv'
result.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")