# filename: filter_and_adjust_budget_with_reason.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv')

# 将日期转换为 datetime 类型
data['date'] = pd.to_datetime(data['date'])

# 计算最近7天的总销售
data['sales_7d'] = data.groupby(['campaignId']).apply(lambda x: x.set_index('date')['sales'].rolling('7D', min_periods=1).sum())

# 定义筛选条件
def filter_and_adjust_budget(df):
    # 定义一
    condition1 = (df['avg_ACOS_7d'] > 0.24) & (df['ACOS'] > 0.24) & (df['clicks'] >= 10) & (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
    # 定义二
    condition2 = (df['avg_ACOS_7d'] > 0.24) & (df['ACOS'] > 0.24) & (df['cost'] > df['Budget'] * 0.8) & (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])
    # 定义三
    condition3 = (df['avg_ACOS_1m'] > 0.24) & (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']) & (df['sales_7d'] == 0) & (df['clicks_7d'] >= 15)
    
    # 应用条件并调整预算
    df['new_budget'] = df['Budget']
    df.loc[condition1 | condition2 | condition3, 'new_budget'] = df.loc[condition1 | condition2 | condition3, 'new_budget'] - 5
    df.loc[df['new_budget'] < 8, 'new_budget'] = 8
    df.loc[condition3 & (df['new_budget'] >= 5), 'new_budget'] = 5
    
    # 添加 reason 列
    df['reason'] = ''
    df.loc[condition1, 'reason'] = '满足定义一'
    df.loc[condition2, 'reason'] = '满足定义二'
    df.loc[condition3, 'reason'] = '满足定义三'
    
    return df

# 应用筛选和预算调整
filtered_data = filter_and_adjust_budget(data)

# 输出结果到CSV文件
output_filename = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\劣质广告活动_FR_2024-5-28_deepseek.csv'
filtered_data[['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'clicks_7d', 'sales_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_budget', 'reason']].to_csv(output_filename, index=False)

print("Output saved to:", output_filename)