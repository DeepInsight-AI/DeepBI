# filename: generate_echarts_data.py

import pandas as pd
import json

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 确定今天和昨天的日期
today = pd.to_datetime('2024-05-28')
yesterday = today - pd.Timedelta(days=1)

# 转换日期列为datetime类型
data['date'] = pd.to_datetime(data['date'])

# 筛选昨天的数据
yesterday_data = data[data['date'] == yesterday]

# 定义判断条件
def condition_1(row):
    condition = (
        row['avg_ACOS_7d'] > 0.24 and
        row['ACOS'] > 0.24 and
        row['clicks'] >= 10 and
        row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']
    )
    return condition

def condition_2(row):
    condition = (
        row['avg_ACOS_7d'] > 0.24 and
        row['ACOS'] > 0.24 and
        row['cost'] > 0.8 * row['Budget'] and
        row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']
    )
    return condition

def condition_3(row):
    condition = (
        row['avg_ACOS_1m'] > 0.24 and
        row['avg_ACOS_1m'] > row['country_avg_ACOS_1m'] and
        row['sales_1m'] == 0 and
        row['clicks_7d'] >= 15
    )
    return condition

# 筛选符合条件的广告活动
filtered_data = yesterday_data[
    yesterday_data.apply(lambda row: condition_1(row) or condition_2(row) or condition_3(row), axis=1)
]

# 调整预算
def adjust_budget(row):
    new_budget = row['Budget']
    reason = None
    if condition_1(row) or condition_2(row):
        new_budget = max(8, row['Budget'] - 5)
        reason = '定义一' if condition_1(row) else '定义二'
    elif condition_3(row):
        new_budget = max(5, row['Budget'] - 5)
        reason = '定义三'
    return new_budget, reason

filtered_data[['new_budget', 'reason']] = filtered_data.apply(lambda row: adjust_budget(row), axis=1, result_type='expand')

# 生成Echarts所需的JSON数据格式
echarts_data = {
    "title": {
        "text": "表现较差的劣质广告活动信息"
    },
    "tooltip": {
        "trigger": "axis"
    },
    "legend": {
        "data": ["预算", "新预算", "点击数", "ACOS"]
    },
    "xAxis": {
        "type": "category",
        "data": filtered_data['campaignName'].tolist()
    },
    "yAxis": [
        {
            "type": "value",
            "name": "预算",
        },
        {
            "type": "value",
            "name": "ACOS",
        }
    ],
    "series": [
        {
            "name": "预算",
            "type": "bar",
            "data": filtered_data['Budget'].tolist()
        },
        {
            "name": "新预算",
            "type": "bar",
            "data": filtered_data['new_budget'].tolist()
        },
        {
            "name": "点击数",
            "type": "line",
            "data": filtered_data['clicks'].tolist(),
            "yAxisIndex": 1
        },
        {
            "name": "ACOS",
            "type": "line",
            "data": filtered_data['ACOS'].tolist(),
            "yAxisIndex": 1
        }
    ]
}

# 保存为json文件
output_path_json = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\ad_performance_echarts_data.json'
with open(output_path_json, 'w', encoding='utf-8') as f:
    json.dump(echarts_data, f, ensure_ascii=False, indent=4)

print("处理完成，Echarts数据已保存至", output_path_json)