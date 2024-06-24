# filename: optimal_ads_bidding_strategy.py

import pandas as pd
from datetime import datetime, timedelta

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 当前日期
current_date = datetime(2024, 5, 27)
yesterday = current_date - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 过滤满足定义一条件的广告位
result_rows = []

for campaign, group in df.groupby('campaignName'):
    group = group.reset_index(drop=True)
    min_acos_7d = group['ACOS_7d'].min()
    min_acos_3d = group['ACOS_3d'].min()

    for i, row in group.iterrows():
        if (row['ACOS_7d'] == min_acos_7d and 0 < row['ACOS_7d'] <= 0.24 and row['total_clicks_7d'] != group['total_clicks_7d'].max()) and \
           (row['ACOS_3d'] == min_acos_3d and 0 < row['ACOS_3d'] <= 0.24 and row['total_clicks_3d'] != group['total_clicks_3d'].max()):
            
            increase_bid_reason = "最近3天和7天平均ACOS值最小, 并且ACOS值在0到0.24之间且点击次数不是最大"
            
            result_rows.append({
                'date': yesterday_str,
                'campaignName': row['campaignName'],
                '广告位': row['placementClassification'],
                '最近7天的平均ACOS值': row['ACOS_7d'],
                '最近3天的平均ACOS值': row['ACOS_3d'],
                '最近7天的总点击次数': row['total_clicks_7d'],
                '最近3天的总点击次数': row['total_clicks_3d'],
                '竞价操作': '提高竞价5%',
                '原因': increase_bid_reason
            })

# 转换为DataFrame
result_df = pd.DataFrame(result_rows)

# 保存结果到CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\优质广告位_FR_2024-5-27.csv'
result_df.to_csv(output_path, index=False)

print(f"结果已保存到CSV文件中：{output_path}")