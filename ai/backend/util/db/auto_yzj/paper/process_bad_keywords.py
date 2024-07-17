# filename: process_bad_keywords.py
import pandas as pd
import numpy as np

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
try:
    data = pd.read_csv(file_path)
    print("数据读取成功。")
except Exception as e:
    print(f"读取数据时发生错误: {e}")
    exit()

# 确保读取正确的字段
required_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'targeting',
    'total_cost_yesterday', 'total_clicks_yesterday', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_30d', 
    'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d', 'total_clicks_7d'
]

missing_columns = [col for col in required_columns if col not in data.columns]
if missing_columns:
    print(f"数据集缺少以下字段: {missing_columns}")
    exit()

# 填充缺失值以避免计算错误
data.fillna(0, inplace=True)

# 初始化新列
data['New_keywordBid'] = data['keywordBid']
data['操作原因'] = np.nan

# 定义条件和操作
conditions_actions_reasons = [
    (
        (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] <= 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.5),
        data['keywordBid'] / (((data['ACOS_7d'] - 0.24) / 0.24) + 1),
        '定义一：竞价调整'
    ),
    (
        (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36),
        data['keywordBid'] / (((data['ACOS_7d'] - 0.24) / 0.24) + 1),
        '定义二：竞价调整'
    ),
    (
        (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36),
        data['keywordBid'] - 0.04,
        '定义三：竞价调整'
    ),
    (
        (data['total_clicks_7d'] > 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5),
        '关闭',
        '定义四：关闭'
    ),
    (
        (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36),
        '关闭',
        '定义五：关闭'
    ),
    (
        (data['total_sales14d_30d'] == 0) & (data['total_cost_30d'] >= 5),
        '关闭',
        '定义六：关闭'
    ),
    (
        (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15) & (data['total_clicks_7d'] > 0),
        '关闭',
        '定义七：关闭'
    )
]

# 应用条件和操作
for condition, action, reason in conditions_actions_reasons:
    data.loc[condition, 'New_keywordBid'] = action
    data.loc[condition, '操作原因'] = reason
    print(f"应用规则 - {reason}")

# 筛选被识别的商品投放
result = data.dropna(subset=['操作原因'])

# 选择需要的字段
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 'targeting',
    'total_cost_yesterday', 'total_clicks_yesterday', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_30d', 
    'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', '操作原因'
]

result = result[output_columns]

# 输出结果到CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_US_2024-07-04.csv'
try:
    result.to_csv(output_file_path, index=False)
    print(f"处理完成，结果已保存到 {output_file_path}")
except Exception as e:
    print(f"保存结果时发生错误: {e}")
    exit()