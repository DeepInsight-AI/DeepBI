# filename: gutana_adver_analysis.py
import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 今日日期假设为2024-05-27
current_date = pd.to_datetime('2024-05-27')

# 分类函数定义
def classify_acos(row):
    if pd.isna(row):
        return '无穷大'
    elif row < 0.2:
        return '极低'
    elif row < 0.3:
        return '较低'
    elif row < 0.5:
        return '较高'
    else:
        return '极高'

# 分类ACOS值
data['ACOS分类'] = data['ACOS_7d'].apply(classify_acos)

# 定义条件
condition1 = (data['ACOS_7d'] >= 0.3) & (data['total_clicks_7d'] > 0) & (data['total_sales14d_7d'] == 0)
condition2 = (data['ACOS_7d'] >= 0.5) & (data['total_sales14d_7d'] == 0)
condition3 = (data['total_clicks_30d'] > 10) & (data['total_cost_30d'] > 0) & (data['total_sales14d_30d'] == 0)

# 过滤满足条件广告活动
filtered_data = data[condition1 | condition2 | condition3].copy()

# 添加原因列
filtered_data.loc[condition1, '原因'] = 'ACOS值较高，点击数较多，销售额占比少'
filtered_data.loc[condition2, '原因'] = 'ACOS值极高，销售额占比少'
filtered_data.loc[condition3, '原因'] = '近一个月点击超10次，有花费没销售额'

# 输出需要的字段
output_columns = [
    'campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 
    'total_clicks_30d', 'searchTerm', 'matchType', '原因'
]
filtered_data = filtered_data[output_columns]
filtered_data.columns = ['广告活动', '广告组', '近七天的花费', '近七天的ACOS值', '近一个月的总点击数', '搜索词', '匹配类型', '原因']

# 保存结果文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_劣质搜索词_IT_2024-06-06.csv'
filtered_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print("文件已成功保存：", output_path)