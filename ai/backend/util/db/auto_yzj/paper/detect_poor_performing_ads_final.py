# filename: detect_poor_performing_ads_final.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 保留需要的字段
fields = [
    "placementClassification",
    "campaignName",
    "total_clicks_3d",
    "total_clicks_7d",
    "total_sales14d_7d",
    "ACOS_3d",
    "ACOS_7d"
]
data = data[fields]

# 定义一
condition1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)
result1 = data[condition1].copy()
result1['reason'] = '定义一'

# 定义二
grouped = data.groupby('campaignName')
result2_list = []
for name, group in grouped:
    if len(group) >= 3:
        filtered_group = group[(group['ACOS_7d'] > 0.24) & (group['ACOS_7d'] < 0.50)]
        if len(filtered_group) == 3:
            max_acos = filtered_group['ACOS_7d'].max()
            min_acos = filtered_group['ACOS_7d'].min()
            if max_acos - min_acos >= 0.2:
                max_acos_placement = filtered_group[filtered_group['ACOS_7d'] == max_acos]
                max_acos_placement['reason'] = '定义二'
                result2_list.append(max_acos_placement)
result2 = pd.concat(result2_list) if result2_list else pd.DataFrame(columns=data.columns)

# 定义三
condition3 = (data['ACOS_7d'] >= 0.50)
result3 = data[condition3].copy()
result3['reason'] = '定义三'

# 合并结果
final_result = pd.concat([result1, result2, result3])
final_result = final_result.drop_duplicates().reset_index(drop=True)

# 输出到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_IT_2024-06-11.csv'
final_result.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到：{output_path}")
print(final_result.head())