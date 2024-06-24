# filename: task_ads_filter.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义ACOS分类
def acos_category(acos):
    if pd.isnull(acos) or acos == float('inf'):
        return '无穷大'
    if acos < 20:
        return '极低'
    elif 20 <= acos < 30:
        return '较低'
    elif 30 <= acos < 50:
        return '较高'
    else:
        return '极高'

# 增加分类列
data['ACOS_30d_category'] = data['ACOS_30d'].apply(acos_category)
data['ACOS_7d_category'] = data['ACOS_7d'].apply(acos_category)
data['ACOS_yesterday_category'] = data['ACOS_yesterday'].apply(acos_category)

# 定义原因判定方法
def determine_reason(row):
    reasons = []
    if row['ACOS_7d_category'] == '较高' and row['total_clicks_7d'] > 0 and row['total_sales14d_7d'] < 0.1 * row['total_clicks_7d']:
        reasons.append('符合定义一：ACOS较高，点击次数较多，销售额占比较少')
    
    if row['ACOS_7d_category'] == '极高' and row['total_sales14d_7d'] < 0.1 * row['total_clicks_7d']:
        reasons.append('符合定义二：ACOS极高，销售额占比较少')
    
    if row['total_clicks_30d'] > 10 and row['total_cost_30d'] > 0 and row['total_sales14d_30d'] == 0:
        reasons.append('符合定义三：近一个月点击超过10次，有花费但无销售额')
    
    return '; '.join(reasons)

# 应用原因判定
data['reason'] = data.apply(determine_reason, axis=1)

# 筛选出符合条件的行
filtered_data = data[data['reason'] != '']

# 输出结果
output_columns = ['campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'reason']
filtered_data = filtered_data[output_columns]

# 保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\劣质搜索词_FR.csv'
filtered_data.to_csv(output_file_path, index=False)

print("脚本执行完成，结果已保存到指定位置。")