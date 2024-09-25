# filename: extract_sku.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 文件路径定义
    input_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
    file_name = "SD_关闭sdSKU" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)

    # 读取CSV数据
    df = pd.read_csv(input_csv_path)

    if version == 1:
        # 定义筛选条件
        condition1 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_cost_7d'] > 5)
        condition2 = (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales_7d'] == 0) & (df['total_cost_7d'] > 5)
        condition3 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'].between(0.24, 0.5)) & (df['ACOS_30d'].between(0, 0.24)) & (df['total_cost_7d'] > 5)
        condition4 = (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
        condition5 = df['ACOS_7d'] > 0.5
        condition6 = (df['total_cost_30d'] > 5) & (df['total_sales_30d'] == 0)
        condition7 = (df['ORDER_1m'] < 8) & (df['total_cost_7d'] >= 5) & (df['total_sales_7d'] == 0)
        condition8 = (df['ORDER_1m'] >= 8) & (df['total_cost_7d'] >= 10) & (df['total_sales_7d'] == 0)

    # 筛选满足任意一个定义条件的sku
        filtered_df = df[condition1 | condition2 | condition3 | condition4 | condition5 | condition6 | condition7 | condition8]

        # 添加满足的定义列
        filtered_df['match_definition'] = ''
        filtered_df.loc[condition1, 'match_definition'] += '定义一,'
        filtered_df.loc[condition2, 'match_definition'] += '定义二,'
        filtered_df.loc[condition3, 'match_definition'] += '定义三,'
        filtered_df.loc[condition4, 'match_definition'] += '定义四,'
        filtered_df.loc[condition5, 'match_definition'] += '定义五,'
        filtered_df.loc[condition6, 'match_definition'] += '定义六,'
        filtered_df.loc[condition7, 'match_definition'] += '定义七,'
        filtered_df.loc[condition8, 'match_definition'] += '定义八,'
        filtered_df['match_definition'] = filtered_df['match_definition'].str.rstrip(',')
    elif version == 2:
        # 定义筛选条件
        condition1 = (df['total_cost_30d'] > 15) & (df['total_sales_30d'] == 0)

    # 筛选满足任意一个定义条件的sku
        filtered_df = df[condition1]

        # 添加满足的定义列
        filtered_df['match_definition'] = ''
        filtered_df.loc[condition1, 'match_definition'] += '定义一,'
        filtered_df['match_definition'] = filtered_df['match_definition'].str.rstrip(',')

    # 选择指定列
    output_columns = [
        'campaignName', 'adGroupName', 'adId', 'ACOS_30d','total_sales_30d','total_cost_30d',
        'ACOS_7d','total_sales_7d','total_cost_7d','advertisedSku', 'ORDER_1m', 'match_definition'
    ]
    result_df = filtered_df[output_columns]
    result_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in result_df.iterrows():
        api.create_sku_info(country, brand, '日常优化', 'SD_关闭', row['campaignName'],row['adGroupName'],
                               row['adId'], row['ACOS_30d'], None,
                               row['total_sales_30d'], row['total_cost_30d'], row['ACOS_7d'], None, row['total_sales_7d'],
                               row['total_cost_7d'], row['advertisedSku'], row['ORDER_1m'], row['match_definition'], cur_time,
                               datetime.now(), 0)
    # 保存结果到CSV文件
    result_df.to_csv(output_file_path, index=False)

    print(f"Filtered data has been saved to {output_file_path}")

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-18','LAPASA','2024-07-18','US')
