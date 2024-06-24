# filename: analysis_bad_adwords.py
import pandas as pd
import numpy as np

# 定义文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_劣质搜索词_IT_2024-06-05.csv'

# 读取 CSV 文件
df = pd.read_csv(file_path)

# ACOS 定义
def acos_category(acos):
    if pd.isna(acos) or acos == np.inf:
        return 'inf'
    elif acos < 20:
        return 'low'
    elif 20 <= acos < 30:
        return 'medium_low'
    elif 30 <= acos < 50:
        return 'medium_high'
    elif acos >= 50:
        return 'high'
    else:
        return 'undefined'

# 添加 ACOS 分类列
df['acos_category'] = df['ACOS_7d'].apply(acos_category)
df['sale_percentage_7d'] = df['total_sales14d_7d'] / df['total_sales14d_30d'].replace({0: np.nan})

# 定义1筛选
cond1 = (df['acos_category'] == 'medium_high') & (df['total_clicks_7d'] > 10) & (df['sale_percentage_7d'] < 0.1)
# 定义2筛选
cond2 = (df['acos_category'] == 'high') & (df['sale_percentage_7d'] < 0.05)
# 定义3筛选
cond3 = (df['total_clicks_30d'] > 10) & (df['total_cost_7d'] > 0) & (df['total_sales14d_7d'] == 0)

# 合并所有条件
df['reason'] = np.select([cond1, cond2, cond3], 
                         ['高ACOS高点击低销售额占比', '极高ACOS低销售额占比', '点击多花费有无销售'], 
                         default=None)

# 筛选满足过滤条件的数据
result = df.dropna(subset=['reason'])

# 选择需要的列
result = result[['campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'reason']]

# 重命名列
result.columns = ['Campaign Name', 'adGroupName', 'cost_7d', 'week_acos', 'sum_clicks', 'searchTerm', 'reason']

# 保存结果到新文件
result.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"处理完成，结果已保存到 {output_path}")