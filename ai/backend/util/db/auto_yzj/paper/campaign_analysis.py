# filename: campaign_analysis.py

import pandas as pd

# 读取 CSV 文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
data = pd.read_csv(file_path)

# 筛选满足条件的广告位
def get_best_placements(group):
    # 获取最小7天ACOS的广告位
    valid_acos_7d = group[(group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 24)]
    valid_acos_3d = group[(group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 24)]
    
    if not valid_acos_7d.empty and not valid_acos_3d.empty:
        min_acos_7d = valid_acos_7d['ACOS_7d'].idxmin()
        min_acos_3d = valid_acos_3d['ACOS_3d'].idxmin()
        max_clicks_7d = group['total_clicks_7d'].idxmax()
        
        if min_acos_7d != max_clicks_7d and min_acos_3d != max_clicks_7d:
            result = group.loc[[min_acos_7d, min_acos_3d]]
            result['竞价操作'] = (result['total_cost_7d'] + result['total_cost_3d']) * 1.05
            result['竞价操作'] = result['竞价操作'].clip(upper=1.5)
            result['对广告位进行竞价操作的原因'] = "满足定义一: 7天和3天ACOS值最小并且点击数不是最大的广告位"
            return result
    return pd.DataFrame()

# 应用筛选函数
filtered_data = data.groupby('campaignName').apply(get_best_placements).reset_index(drop=True)

# 选择所需的列
columns_to_save = [
    'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 
    'total_clicks_7d', 'total_clicks_3d', '竞价操作', '对广告位进行竞价操作的原因'
]
result = filtered_data[columns_to_save]

# 保存结果到新的 CSV 文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\优质广告位_FR.csv"
result.to_csv(output_file_path, index=False)

# 显示前几行结果以确认
print(result.head())