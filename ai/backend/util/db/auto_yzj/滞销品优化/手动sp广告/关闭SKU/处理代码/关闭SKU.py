# filename: extract_sku.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 文件路径定义
    input_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关闭SKU\预处理.csv'
    file_name = "手动_关闭SKU" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)

    # 读取CSV数据
    df = pd.read_csv(input_csv_path)
    if version == 1:
        # 定义筛选条件
        condition1 = (df['ORDER_1m'] < 5) & (df['ACOS_7d'] > 0.6) & (df['total_clicks_7d'] > 13)
        condition2 = (df['ORDER_1m'] < 5) & (df['ACOS_30d'] > 0.6) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 13)
        condition3 = (df['ORDER_1m'] < 5) & (df['ACOS_7d'] > 0.6) & (df['ACOS_30d'] > 0.6)
        condition4 = (df['total_clicks_30d'] > 50) & (df['total_sales14d_30d'] == 0)
        condition5 = df['ORDER_1m'] < 5 & (df['total_clicks_7d'] >= 19) & (df['total_sales14d_7d'] == 0)
        condition6 = (df['total_clicks_7d'] >= 30) & (df['total_sales14d_7d'] == 0)


        # 筛选满足任意一个定义条件的sku
        filtered_df = df[condition1 | condition2 | condition3 | condition4 | condition5 | condition6]

        # 添加满足的定义列
        filtered_df['match_definition'] = ''
        filtered_df.loc[condition1, 'match_definition'] += '定义一,'
        filtered_df.loc[condition2, 'match_definition'] += '定义二,'
        filtered_df.loc[condition3, 'match_definition'] += '定义三,'
        filtered_df.loc[condition4, 'match_definition'] += '定义四,'
        filtered_df.loc[condition5, 'match_definition'] += '定义五,'
        filtered_df.loc[condition6, 'match_definition'] += '定义六,'
        filtered_df['match_definition'] = filtered_df['match_definition'].str.rstrip(',')
    elif version == 2:
        condition1 = (df['total_cost_30d'] > 10) & (df['total_sales14d_30d'] == 0)
        condition2 = (df['total_clicks_30d'] > 20) & (df['total_sales14d_30d'] == 0)

        filtered_df = df[condition1 | condition2]

        # 添加满足的定义列
        filtered_df['match_definition'] = ''
        filtered_df.loc[condition1, 'match_definition'] += '定义一,'
        filtered_df.loc[condition2, 'match_definition'] += '定义二,'
    # 选择指定列
    output_columns = [
        'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d',
        'advertisedSku', 'ORDER_1m', 'match_definition'
    ]
    result_df = filtered_df[output_columns]
    result_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in result_df.iterrows():
        api.create_sku_info(country, brand, '滞销品优化', '手动_关闭', row['campaignName'], row['adGroupName'],
                            row['adId'], row['ACOS_30d'], None, None, None, row['ACOS_7d'], row['total_clicks_7d'],
                            None, None, row['advertisedSku'], row['ORDER_1m'], row['match_definition'],
                            cur_time, datetime.now(), 0)
    # 保存结果到CSV文件
    result_df.to_csv(output_file_path, index=False)

    print(f"Filtered data has been saved to {output_file_path}")
