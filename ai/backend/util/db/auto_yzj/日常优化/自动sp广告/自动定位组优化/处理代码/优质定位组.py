# filename: optimize_keywords.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version=3):
    # 读取数据集
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
    file_name = "自动_优质定位组" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    if version == 1:
        # 定义调整竞价的条件并执行提价操作
        conditions = [
            {
                'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0.5),
                'increment': 0.01,
                'reason': '定义一：7天ACOS在 0 与 0.24 之间且 30天ACOS 大于 0.5'
            },
            {
                'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.5),  # Same as 条件一
                'increment': 0.02,
                'reason': '定义二：7天ACOS在 0 与 0.24 之间且30天ACOS在 0.24 与 0.5 之间'
            },
            {
                'condition': (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24),
                'increment': 0.03,
                'reason': '定义三：7天ACOS在 0.1 与 0.24 之间且30天ACOS在 0 与 0.24 之间'
            },
            {
                'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.1) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24),
                'increment': 0.05,
                'reason': '定义四：7天ACOS在 0 与 0.1 之间且30天ACOS在 0 与 0.24 之间'
            }
        ]
    elif version == 2:
        conditions = [
            {
                'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0.5) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
                'increment': 0.01,
                'reason': '定义一：7天ACOS在 0 与 0.24 之间且 30天ACOS 大于 0.5'
            },
            {
                'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0.24) & (
                        data['ACOS_30d'] < 0.5) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),  # Same as 条件一
                'increment': 0.02,
                'reason': '定义二：7天ACOS在 0 与 0.24 之间且30天ACOS在 0.24 与 0.5 之间'
            },
            {
                'condition': (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0) & (
                        data['ACOS_30d'] < 0.24) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
                'increment': 0.03,
                'reason': '定义三：7天ACOS在 0.1 与 0.24 之间且30天ACOS在 0 与 0.24 之间'
            },
            {
                'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.1) & (data['ACOS_30d'] > 0) & (
                        data['ACOS_30d'] < 0.24) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2),
                'increment': 0.05,
                'reason': '定义四：7天ACOS在 0 与 0.1 之间且30天ACOS在 0 与 0.24 之间'
            }
        ]
    elif version == 3:
        conditions = [
            {
                'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['ACOS_3d'] > 0) & (data['ACOS_3d'] < 0.24),
                'increment': 0.02,
                'reason': '定义一'
            }
        ]

    results = []

    for condition in conditions:
        filtered_data = data[condition['condition']]
        for _, row in filtered_data.iterrows():
            New_keywordBid = row['bid'] + condition['increment']
            results.append([
                row['campaignName'],
                row['adGroupName'],
                row['keyword'],
                row['keywordId'],
                row['bid'],
                New_keywordBid,
                row['ACOS_30d'],
                row['ACOS_7d'],
                row['ACOS_3d'],
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
        'ACOS_3d',
        'bid_adjust',
        'reason'
    ]

    results_df = pd.DataFrame(results, columns=columns)
    results_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in results_df.iterrows():
        api.create_automatic_targeting_info(country, brand, '日常优化', '自动_优质', row['keyword'], row['keywordId'],
                                row['campaignName'], row['adGroupName'], row['keywordBid'],
                                row['New_keywordBid'], row['ACOS_30d'], None,
                                None, None,None,
                                row['ACOS_7d'], None, None, None,
                                row['ACOS_3d'], None, None, row['reason'], row['bid_adjust'],
                                cur_time,
                                datetime.now(), 0)
    # 将结果保存到CSV文件
    results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

    print(f"Results have been saved to {output_file_path}")

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-16','LAPASA','2024-07-16','US')
