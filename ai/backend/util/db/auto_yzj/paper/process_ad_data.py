# filename: process_ad_data.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 确定今天的日期并计算时间窗口
today = pd.Timestamp('2024-05-27')

# 定义判断条件
def reason_judgment(row):
    try:
        acos_30d = float(row['ACOS_30d'].strip('%')) / 100
        acos_7d = float(row['ACOS_7d'].strip('%')) / 100
    except:
        acos_30d = float('inf')
        acos_7d = float('inf')
        
    try:
        cost_7d = float(row['total_cost_7d'])
        sales_7d = float(row['total_sales14d_7d'])
        clicks_7d = int(row['total_clicks_7d'])
        clicks_30d = int(row['total_clicks_30d'])
    except:
        return None
    
    reasons = []
    
    # 定义一
    if acos_30d >= 0.30 and clicks_7d > 5 and sales_7d < 0.01 * cost_7d:
        reasons.append('定义一')
    
    # 定义二
    if acos_30d >= 0.50 and sales_7d < 0.05 * cost_7d:
        reasons.append('定义二')
    
    # 定义三
    if clicks_30d > 10 and cost_7d > 0 and sales_7d == 0:
        reasons.append('定义三')
        
    if reasons:
        return ','.join(reasons)
    else:
        return None

data['reason'] = data.apply(reason_judgment, axis=1)

# 筛选符合条件的行
filtered_data = data.dropna(subset=['reason'])

selected_columns = ['campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'reason']
output_data = filtered_data[selected_columns]

# 保存结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_劣质搜索词_ES_2024-06-121.csv'
output_data.to_csv(output_path, index=False)

print(f"结果已保存至 {output_path}")