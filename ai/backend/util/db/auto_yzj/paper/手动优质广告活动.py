# filename: campaign_budget_update.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选符合条件的广告活动
conditions = (
    (df['ACOS_7d'] < 0.24) & 
    (df['ACOS_yesterday'] < 0.24) & 
    (df['cost_yesterday'] > 0.8 * df['Budget'])
)
good_campaigns = df[conditions].copy()

# 更新预算
good_campaigns['New_Budget'] = good_campaigns['Budget'] * 1.2
good_campaigns['New_Budget'] = good_campaigns['New_Budget'].apply(lambda x: min(x, 50))

# 添加原因
good_campaigns['Reason'] = "提高预算，因为该广告表现良好，最近7天和昨天的ACOS值都在0.24以下且昨天花费超过预算80%"

# 保留需要的字段并重命名
output_df = good_campaigns[[
    'campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday', 
    'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 
    'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d', 'Reason'
]].copy()

# 修改列名
output_df.rename(columns={
    'cost_yesterday': 'cost',
    'clicks_yesterday': 'clicks',
    'ACOS_yesterday': 'ACOS'
}, inplace=True)

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_v1_1_IT_2024-06-13.csv'
output_df.to_csv(output_file_path, index=False)

print("文件已成功保存到:", output_file_path)