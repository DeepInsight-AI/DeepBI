# filename: optimize_ad_campaign_budget.py

import pandas as pd
from datetime import datetime

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path, encoding='utf-8')

# 筛选出符合条件的广告活动
filtered_data = data[
    (data['ACOS_7d'] < 0.27) &
    (data['ACOS_yesterday'] < 0.27) &
    (data['cost_yesterday'] > 0.8 * data['Budget'])
].copy()

# 增加预算
filtered_data['New Budget'] = filtered_data['Budget'].apply(lambda x: min(x * 1/5 + x, 50))

# 添加原因
filtered_data['Reason'] = 'Increase Budget due to high performance and overspend yesterday'

# 选择需要输出的列
output_columns = [
    'campaignId', 'campaignName', 'Budget', 'New Budget', 'cost_yesterday', 'clicks_yesterday',
    'ACOS_yesterday', 'ACOS_7d', 'country_avg_ACOS_1m', 'total_clicks_30d', 'total_sales14d_30d', 'Reason'
]

# 生成输出路径和文件名
current_date = datetime.now().strftime('%Y-%m-%d')
output_file_path = f'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\滞销品优化\\手动sp广告\\预算优化\\提问策略\\手动_优质广告活动_v1_1_LAPASA_ES_{current_date}.csv'

# 保存到新的CSV文件
filtered_data[output_columns].to_csv(output_file_path, index=False, encoding='utf-8')

print(f'Output saved to {output_file_path}')