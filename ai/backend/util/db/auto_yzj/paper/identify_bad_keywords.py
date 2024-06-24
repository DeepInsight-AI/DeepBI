# filename: identify_bad_keywords.py
import pandas as pd

# 读取csv文件
df = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv')

# 定义判定标准
def identify_keyword(row):
    reasons = []
    if row['ACOS_7d'] > 0.5:  # acos极高
        if (row['total_sales14d_7d'] == 0 or row['total_sales14d_7d'] / row['total_sales14d_30d'] < 0.1):
            reasons.append("acos较高，销售额占比相对极少")
        if row['total_clicks_30d'] > 10 and row['total_sales14d_30d'] == 0 and row['total_cost_30d'] > 0:
            reasons.append("近一个月点击次数超过10次，有花费但是没销售额")

    if row['ACOS_7d'] > 0.3 and row['total_clicks_7d'] > 5 and (row['total_sales14d_7d'] == 0 or row['total_sales14d_7d'] / row['total_sales14d_30d'] < 0.1):
        reasons.append("acos值较高，点击次数相对较多，销售额占比相对极少")
    
    return ";".join(reasons)

# 筛选符合条件的行
df['reason'] = df.apply(identify_keyword, axis=1)
result_df = df[df['reason'] != '']

# 构建结果集
result_df = result_df[['campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'reason']].copy()
result_df.columns = ['Campaign Name', 'adGroupName', 'cost_7d', 'week_acos', 'sum_clicks', 'searchTerm', 'reason']

# 保存结果到新的csv文件
result_df.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_劣质搜索词_ES_2024-06-10.csv', index=False)

print("文件生成成功")