# filename: close_auto_group_keywords.py

import pandas as pd

# 读取数据
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(data_path)

# 定义判定条件

def satisfies_condition(row):
    # 定义一
    if (row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0 and 
        row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] > 10):
        return '定义一'
    
    # 定义二
    if (row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0 and 
        row['ACOS_30d'] > 0.5):
        return '定义二'
    
    # 定义三
    if (row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.24):
        return '定义三'
    
    return None

# 筛选需关闭的关键词
close_keywords = data.apply(lambda row: {
    'campaignName': row['campaignName'],
    'adGroupName': row['adGroupName'],
    'keyword': row['keyword'],
    'ACOS_30d': row['ACOS_30d'],
    'ACOS_7d': row['ACOS_7d'],
    '提价的原因': satisfies_condition(row)
}, axis=1)

# 去除不满足三条定义之一的行
result = pd.DataFrame([kw for kw in close_keywords if kw['提价的原因']])

# 保存结果到新CSV文件
result_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\关闭自动定位组_ES_2024-6-04.csv'
result.to_csv(result_path, index=False, encoding='utf-8-sig')

print("处理完毕，结果已保存至:", result_path)