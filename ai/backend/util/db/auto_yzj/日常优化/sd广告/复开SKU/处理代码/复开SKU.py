# filename: process_sku_data.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 定义文件路径
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\复开SKU\预处理.csv'
    file_name = "SD_复开sdSKU" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)

    # 读取数据
    df = pd.read_csv(file_path)
    if version == 1:
        # 定义一个函数来判断每一行是否符合定义一或定义二
        def is_valid_sku(row):
            condition1 = row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.24 and row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.27
            condition2 = row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.24 and row['total_cost_7d'] < 3
            return condition1 or condition2

        # 筛选符合条件的行
        valid_skus = df[df.apply(is_valid_sku, axis=1)]

        # 选择需要输出的列
        output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_cost_7d', 'advertisedSku']
        if valid_skus.empty:
            valid_skus = valid_skus[output_columns]

            # 添加一列 "满足的定义" 描述是定义一还是定义二
            valid_skus['Reason'] = valid_skus.apply(
                lambda row: '定义一' if row['ACOS_30d'] <= 0.24 and row['ACOS_7d'] <= 0.27
                else '定义二' if row['ACOS_30d'] <= 0.24 and row['total_cost_7d'] < 3
                else '不符合', axis=1
            )
    elif version == 2:
        results = []
        for index, row in df.iterrows():
            campaignName = row['campaignName']
            adId = row['adId']
            adGroupName = row['adGroupName']
            ACOS_30d = row['ACOS_30d']
            ACOS_7d = row['ACOS_7d']
            total_sales14d_30d = row['total_sales_30d']
            advertisedSku = row['advertisedSku']
            if total_sales14d_30d > 0:
                reason = '定义一'
                results.append(
                    [campaignName, adId, adGroupName, ACOS_30d, ACOS_7d, total_sales14d_30d, advertisedSku, reason])
        columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_sales14d_30d',
                   'advertisedSku', 'Reason']
        valid_skus = pd.DataFrame(results, columns=columns)

    valid_skus.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in valid_skus.iterrows():
        api.create_sku_info(country, brand, '日常优化', 'SD_复开', row['campaignName'], row['adGroupName'],
                            row['adId'], row['ACOS_30d'], None,row['total_sales14d_30d'],None, row['ACOS_7d'], None,
                            None,None, row['advertisedSku'], None, row['Reason'],
                            cur_time, datetime.now(), 0)
    # 保存结果到新的CSV文件
    valid_skus.to_csv(output_file_path, index=False)

    print(f"筛选后的数据已保存到 {output_file_path}")
#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/OutdoorMaster_SE_2024-07-25','OutdoorMaster','2024-07-25','SE')
