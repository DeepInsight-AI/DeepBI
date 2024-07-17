# filename: optimize_keywords.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country):
    # 读取数据集
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\预处理.csv'
    file_name = "自动_优质定位组" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 定义调整竞价的条件并执行提价操作
    conditions = [
        {
            'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.27) & (data['ACOS_30d'] > 0.5),
            'increment': 0.01,
            'reason': '定义一'
        },
        {
            'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.27) & (data['ACOS_30d'] > 0.27) & (data['ACOS_30d'] < 0.5),  # Same as 条件一
            'increment': 0.02,
            'reason': '定义二'
        },
        {
            'condition': (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] < 0.27) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.27),
            'increment': 0.03,
            'reason': '定义三'
        },
        {
            'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.1) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.27),
            'increment': 0.05,
            'reason': '定义四'
        }
    ]

    results = []

    for condition in conditions:
        filtered_data = data[condition['condition']]
        for _, row in filtered_data.iterrows():
            New_keywordBid = row['keywordBid'] + condition['increment']
            results.append([
                row['campaignName'],
                row['adGroupName'],
                row['keyword'],
                row['keywordId'],
                row['keywordBid'],
                New_keywordBid,
                row['ACOS_30d'],
                row['ACOS_7d'],
                condition['increment'],
                condition['reason']
            ])

    # 创建 DataFrame 存储结果
    columns = [
        'campaignName',
        'adGroupName',
        'keyword',
        'keywordId',
        'keywordBid',
        'New_keywordBid',
        'ACOS_30d',
        'ACOS_7d',
        'increment',
        'reason'
    ]

    results_df = pd.DataFrame(results, columns=columns)
    results_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in results_df.iterrows():
        api.create_automatic_targeting_info(country, brand, '滞销品优化', '自动_优质', row['keyword'], row['keywordId'],
                                row['campaignName'], row['adGroupName'], row['keywordBid'],
                                row['New_keywordBid'], row['ACOS_30d'], None,
                                None, None,
                                row['ACOS_7d'], None, None, None,
                                None, None, None, row['reason'],
                                cur_time,
                                datetime.now(), 0)
    # 将结果保存到CSV文件
    results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

    print(f"Results have been saved to {output_file_path}")
