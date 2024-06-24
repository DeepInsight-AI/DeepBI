# filename: 提价策略.py

import pandas as pd

# 读取csv文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv"
data = pd.read_csv(file_path)

# 表现较好的自动定位词和提价原因的判断及处理
def get_bid_increase_reason(row):
    acos_30d = row['ACOS_30d']
    acos_7d = row['ACOS_7d']
    reason = None
    
    if 0 < acos_7d < 0.24:
        if acos_30d > 0.5:
            if 0.1 < acos_7d < 0.24:
                reason = "提价0.03 - 定义三"
            elif 0 < acos_7d < 0.1:
                reason = "提价0.05 - 定义四"
            else:
                reason = "提价0.02 - 定义二"
        elif 0 < acos_30d < 0.24:
            if 0.1 < acos_7d < 0.24:
                reason = "提价0.03 - 定义三"
            elif 0 < acos_7d < 0.1:
                reason = "提价0.05 - 定义四"
            else:
                reason = "提价0.03 - 定义三"
        else:
            if acos_30d > 0.5:
                reason = "提价0.01 - 定义一"
        
    return reason

# 应用判断条件并生成提价原因
data['提价原因'] = data.apply(get_bid_increase_reason, axis=1)

# 过滤出符合条件的自动定位词
filtered_data = data[~data['提价原因'].isna()]

# 指定输出列
output_columns = ['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', '提价原因']

# 保存到新CSV文件
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_IT_2024-06-08.csv"
filtered_data.to_csv(output_path, columns=output_columns, index=False)

print(f"处理完成，结果已保存至 {output_path}")