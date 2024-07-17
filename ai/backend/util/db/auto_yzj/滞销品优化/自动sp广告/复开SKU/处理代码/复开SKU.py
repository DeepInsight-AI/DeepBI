# filename: sku_filter.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 读取CSV文件路径
    csv_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\复开SKU\预处理.csv"
    file_name = "自动_复开SKU" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(csv_path)

    if version == 1:
        # 定义筛选条件
        condition1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.27)
        condition2 = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.27)
        condition3 = data['total_clicks_7d'] == 0

        # 满足定义一
        definition1 = condition1 & condition2

        # 满足定义二
        definition2 = condition1 & condition3

        # 合并两个定义条件
        final_condition = definition1 | definition2

        # 筛选出满足条件的SKU
        filtered_data = data[final_condition].copy()

        # 加上满足的定义类别
        filtered_data['Reason'] = filtered_data.apply(
            lambda row: 'Definition1' if row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.27 and row['ACOS_7d'] > 0 and row[
                'ACOS_7d'] <= 0.27
            else 'Definition2' if row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.27 and row['total_clicks_7d'] == 0
            else '', axis=1)
    elif version == 2:
        condition1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.27) & (data['ACOS_7d'] > 0) & (
                data['ACOS_7d'] <= 0.27)
        condition2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.27) & (data['total_clicks_7d'] == 0)
        filtered_data = data[condition1 | condition2]

        # 添加满足的定义列
        filtered_data['match_definition'] = ''
        filtered_data.loc[condition1, 'match_definition'] += '定义一,'
        filtered_data.loc[condition2, 'match_definition'] += '定义二,'

    # 创建新的DataFrame来保存需要输出的列
    output_data = filtered_data[
        ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m',
         'match_definition']]
    output_data.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in output_data.iterrows():
        api.create_sku_info(country, brand, '滞销品优化', '自动_复开', row['campaignName'], row['adGroupName'],
                            row['adId'], row['ACOS_30d'], None,None,None, row['ACOS_7d'], row['total_clicks_7d'],
                            None,None, row['advertisedSku'], row['ORDER_1m'], row['Reason'],
                            cur_time, datetime.now(), 0)
    # 定义输出文件路径，并保存结果到新的CSV文件
    output_data.to_csv(output_file_path, index=False)

    print(f"Processed data has been saved to {output_file_path}")
