# filename: filter_and_calculate_bids.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv')

# 确保数据集中包含了所有必要的字段
required_fields = [
    "campaignName", "placementClassification", "total_clicks_7d", "total_clicks_3d",
    "ACOS_7d", "ACOS_3d"
]
missing_fields = [field for field in required_fields if field not in data.columns]
if missing_fields:
    print(f"缺少以下字段: {missing_fields}")
    exit(1)

# 将日期设置为今天的日期
today = "2024-5-27"
data['date'] = today

# 筛选符合条件的广告位
filtered_data = data.copy()

# 定义一的条件
conditions = [
    (filtered_data['ACOS_7d'] > 0) & (filtered_data['ACOS_7d'] <= 0.24),
    (filtered_data['ACOS_3d'] > 0) & (filtered_data['ACOS_3d'] <= 0.24)
]

# 应用条件
filtered_data = filtered_data[conditions[0] & conditions[1]]

# 计算新的竞价
filtered_data['竞价操作'] = filtered_data['ACOS_3d'] * 1.05
filtered_data['竞价操作'] = filtered_data['竞价操作'].apply(lambda x: min(x, 1.5))

# 输出结果
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\优质广告位_FR_2024-5-27_deepseek.csv'
filtered_data[['date', 'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', '竞价操作']].to_csv(output_file, index=False)

# 输出结果确认
print(filtered_data[['date', 'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', '竞价操作']])