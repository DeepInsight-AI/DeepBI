# filename: gutana_analysis.py
import pandas as pd
import numpy as np

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 增加acos分类字段
def categorize_acos(acos):
    if pd.isna(acos) or acos == float('inf'):
        return '无穷大'
    elif acos < 0.2:
        return '极低'
    elif acos < 0.3:
        return '较低'
    elif acos < 0.5:
        return '较高'
    else: 
        return '极高'

# 增加acos分类字段
df['acos_category'] = df['ACOS_7d'].apply(categorize_acos)

# 筛选符合定义的广告活动

# 定义一：acos值较高，并且该关键词点击次数相对较多，销售额占比相对极少
condition_1 = (df['acos_category'] == '较高') & (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] / df['total_clicks_7d'] < 0.1)

# 定义二：acos值极高，销售额占比相对极少
condition_2 = (df['acos_category'] == '极高') & (df['total_sales14d_7d'] / df['total_clicks_7d'] < 0.1)

# 定义三：近一个月的历史点击次数超过10次，有花费但是没销售额
condition_3 = (df['total_clicks_30d'] > 10) & (df['total_cost_30d'] > 0) & (df['total_sales14d_30d'] == 0)

# 合并三个条件
filter_conditions = condition_1 | condition_2 | condition_3
filtered_df = df[filter_conditions].copy()

# 增加原因字段
reasons = []
for idx, row in filtered_df.iterrows():
    if condition_1[idx]:
        reasons.append("匹配定义一")
    elif condition_2[idx]:
        reasons.append("匹配定义二")
    elif condition_3[idx]:
        reasons.append("匹配定义三")
    else:
        reasons.append("其他原因")

filtered_df['reason'] = reasons

# 选择columns
result_df = filtered_df[['campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d',
                         'total_clicks_30d', 'searchTerm', 'matchType', 'reason']].copy()

# 重命名columns
result_df.columns = ['广告活动', '广告组', '近七天的花费', '近七天的acos值', '近一个月的总点击数', '搜索词', '匹配类型', '原因']

# 输出到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\劣质搜索词_FR_2024-5-27.csv'
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("分析结果已保存到：", output_path)