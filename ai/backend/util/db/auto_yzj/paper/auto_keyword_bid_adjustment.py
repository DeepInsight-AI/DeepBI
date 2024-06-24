# filename: auto_keyword_bid_adjustment.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义降价规则函数
def determine_bid_reduction(row):
    ACOS_30d = row['ACOS_30d']
    ACOS_7d = row['ACOS_7d']
    total_sales_7d = row['total_sales14d_7d']
    total_clicks_7d = row['total_clicks_7d']

    if 0.24 < ACOS_7d < 0.5:
        if 0 < ACOS_30d < 0.24:
            return 0.03, "定义一"
        elif 0.24 < ACOS_30d < 0.5:
            return 0.04, "定义二"
        elif ACOS_30d > 0.5:
            return 0.05, "定义四"
    if ACOS_7d > 0.5 and 0 < ACOS_30d < 0.24:
        return 0.05, "定义五"
    if total_sales_7d == 0 and total_clicks_7d > 0 and 0.24 < ACOS_30d < 0.5:
        return 0.04, "定义三"
    return None, None

# 应用规则并过滤符合条件的行
data['bid_reduction'], data['reason'] = zip(*data.apply(determine_bid_reduction, axis=1))
result_data = data.dropna(subset=['bid_reduction'])

# 选择需要的列
output_columns = ['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', 'reason']
output_data = result_data[output_columns]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_IT_2024-06-11.csv'
output_data.to_csv(output_file_path, index=False)

print("Adjustment completed. The result has been saved to:", output_file_path)