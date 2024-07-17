# filename: find_sku.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path)

# 初始化一个空列表来存储符合条件的SKU信息
results = []

# 定义各条件的逻辑判断
def is_definition_1(row):
    return row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['total_cost_7d'] > 5

def is_definition_2(row):
    return row['ORDER_1m'] < 8 and row['ACOS_30d'] > 0.24 and row['total_sales_7d'] == 0 and row['total_cost_7d'] > 5

def is_definition_3(row):
    return row['ORDER_1m'] < 8 and 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24 and row['total_cost_7d'] > 5

def is_definition_4(row):
    return row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['ACOS_30d'] > 0.24

def is_definition_5(row):
    return row['ACOS_7d'] > 0.5

def is_definition_6(row):
    return row['total_cost_30d'] > 5 and row['total_sales_30d'] == 0

def is_definition_7(row):
    return row['ORDER_1m'] < 8 and row['total_cost_7d'] >= 5 and row['total_sales_7d'] == 0

def is_definition_8(row):
    return row['ORDER_1m'] >= 8 and row['total_cost_7d'] >= 10 and row['total_sales_7d'] == 0

# 遍历DataFrame判断每行是否符合定义
for idx, row in df.iterrows():
    definitions = []
    if is_definition_1(row):
        definitions.append('定义一')
    if is_definition_2(row):
        definitions.append('定义二')
    if is_definition_3(row):
        definitions.append('定义三')
    if is_definition_4(row):
        definitions.append('定义四')
    if is_definition_5(row):
        definitions.append('定义五')
    if is_definition_6(row):
        definitions.append('定义六')
    if is_definition_7(row):
        definitions.append('定义七')
    if is_definition_8(row):
        definitions.append('定义八')

    if definitions:
        results.append({
            'campaignName': row['campaignName'],
            'adId': row['adId'],
            'adGroupName': row['adGroupName'],
            'total_acos_30d': row['ACOS_30d'],
            'total_acos_7d': row['ACOS_7d'],
            'total_clicks_7d': row['total_clicks_7d'],
            'advertisedSku': row['advertisedSku'],
            'ORDER_1m': row['ORDER_1m'],
            'definitions': ', '.join(definitions)
        })

# 转换结果为DataFrame
result_df = pd.DataFrame(results)

# 保存到新的CSV文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_IT_2024-07-15.csv'
result_df.to_csv(output_file, index=False)

print("筛选并保存结果完成.")