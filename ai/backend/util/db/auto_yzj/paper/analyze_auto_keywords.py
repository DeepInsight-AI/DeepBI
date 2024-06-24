# filename: analyze_auto_keywords.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义条件一
condition_1 = (
    (data['total_sales14d_7d'] == 0) &
    (data['total_clicks_7d'] > 0) &
    (data['total_sales14d_30d'] == 0) &
    (data['total_clicks_30d'] > 10)
)

# 定义条件二
condition_2 = (
    (data['total_sales14d_7d'] == 0) &
    (data['total_clicks_7d'] > 0) &
    (data['ACOS_30d'] > 0.5)
)

# 定义条件三
condition_3 = (
    (data['ACOS_7d'] > 0.5) &
    (data['ACOS_30d'] > 0.24)
)

# 综合三个条件
filtered_data = data[condition_1 | condition_2 | condition_3].copy()

# 提价原因
reasons = []
for _, row in filtered_data.iterrows():
    reason = []
    if row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0:
        if row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] > 10:
            reason.append("定义一")
        if row['ACOS_30d'] > 0.5:
            reason.append("定义二")
    if row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.24:
        reason.append("定义三")
    reasons.append(','.join(reason))

filtered_data['提价的原因'] = reasons

# 保存目标字段到新CSV文件
output_columns = ['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', '提价的原因']
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_关闭自动定位组_ES_2024-06-10.csv'
filtered_data.to_csv(output_path, columns=output_columns, index=False)

print("CSV文件已保存成功，路径为：", output_path)