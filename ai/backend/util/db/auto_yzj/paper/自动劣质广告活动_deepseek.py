# filename: analyze_ad_campaigns.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv')

# 定义函数来检查每个广告活动是否满足特定条件
def check_conditions(row):
    # 定义一的条件
    if (row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and row['clicks_yesterday'] >= 10 and row['country_avg_ACOS_1m'] < row['ACOS_30d']):
        return '定义一'
    # 定义二的条件
    elif (row['ACOS_7d'] > 0.24 and row['ACOS_yesterday'] > 0.24 and row['cost_yesterday'] > 0.8 * row['Budget'] and row['country_avg_ACOS_1m'] < row['ACOS_30d']):
        return '定义二'
    # 定义三的条件
    elif (row['ACOS_30d'] > 0.24 and row['country_avg_ACOS_1m'] < row['ACOS_30d'] and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] >= 15):
        return '定义三'
    # 定义四的条件
    elif (row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 75):
        return '定义四'
    else:
        return '无'

# 应用函数到每一行
data['Condition'] = data.apply(check_conditions, axis=1)

# 根据条件调整预算
data['New_Budget'] = data['Budget']
for index, row in data.iterrows():
    if row['Condition'] == '定义一' or row['Condition'] == '定义二':
        while row['New_Budget'] > 8:
            data.at[index, 'New_Budget'] -= 5
    elif row['Condition'] == '定义三':
        while row['New_Budget'] > 5:
            data.at[index, 'New_Budget'] -= 5
    elif row['Condition'] == '定义四':
        data.at[index, 'New_Budget'] = '关闭'

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_劣质广告活动_v1_1_IT_2024-06-13_deepseek.csv'
data[['campaignId', 'campaignName', 'Budget', 'New_Budget', 'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 'total_clicks_7d', 'total_sales14d_7d', 'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d', 'country_avg_ACOS_1m', 'Condition']].to_csv(output_path, index=False)

print("处理完成，结果已保存到CSV文件。")