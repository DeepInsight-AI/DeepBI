# filename: deepbi_analysis.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_yzj.path import get_auto_path


def main(path, brand, cur_time, country, db, version=2):
    # 数据路径
    data_path = os.path.join(get_auto_path(), '日常优化\手动sp广告\商品投放搜索词优化\预处理.csv')
    # data_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv"
    file_name = "手动_优质_ASIN_搜索词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)

    # 读取数据
    df = pd.read_csv(data_path)

    if version == 1:
        # 定义筛选条件
        condition1 = (df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)
        condition2 = (df['ORDER_1m'] > 0) & (df['ACOS_30d'] < 0.24)

        # 过滤数据
        filtered_df1 = df[condition1].copy()
        filtered_df1['reason'] = '定义一'

        filtered_df2 = df[condition2 & ~condition1].copy()  # 确保没有重复选中
        filtered_df2['reason'] = '定义二'

        # 合并结果
        result_df = pd.concat([filtered_df1, filtered_df2], ignore_index=True)
    elif version == 2:
        condition1 = (df['ACOS_30d'] < 0.24)

        # 过滤数据
        filtered_df1 = df[condition1].copy()
        filtered_df1['reason'] = '定义一'

        # 合并结果
        result_df = pd.concat([filtered_df1], ignore_index=True)

    result_df['searchTerm'] = result_df['searchTerm'].str.replace("’", "'", regex=False).str.replace("%", "",
                                                                                               regex=False).str.replace(
        "\\", "", regex=False).str.replace("/", "", regex=False)
    # 提取所需列
    columns = [
        'campaignName',
        'campaignId',
        'adGroupName',
        'adGroupId',
        'ACOS_30d',
        'ORDER_1m',
        'ACOS_7d',
        'total_sales14d_7d',
        'total_clicks_30d',
        'total_cost_30d',
        'CPC_30d',
        'searchTerm',
        'reason'
    ]
    result_df = result_df[columns]
    result_df['new_campaignId'] = ''
    result_df['new_campaignName'] = ''
    result_df['new_adGroupId'] = ''
    api2 = DbSpTools(db, brand,country)
    for index, row in result_df.iterrows():
        if len(row['searchTerm']) == 10 and row['searchTerm'].startswith('b0'):
            pass
        # else:
        #     new_campaignId, new_campaignName, new_adGroupId = api2.select_sp_campaignid_search_term(country, cur_time,
        #                                                                                             row['campaignId'])
        #     print(new_campaignId)
        #     print(new_campaignName)
        #     print(new_adGroupId)
        #     result_df.loc[index, 'new_campaignId'] = new_campaignId
        #     result_df.loc[index, 'new_campaignName'] = new_campaignName
        #     result_df.loc[index, 'new_adGroupId'] = new_adGroupId


    result_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(db, brand,country)
    for index, row in result_df.iterrows():
        api.create_product_targets_search_term_info(country, brand, '日常优化', '手动_优质', row['campaignName'],row['campaignId'],
                               row['adGroupName'], row['adGroupId'], row['ACOS_30d'],
                               row['ORDER_1m'],None,None, row['ACOS_7d'], None,None, row['total_sales14d_7d'],
                               None, row['searchTerm'], row['reason'], cur_time,
                               datetime.now(), 0)
    # 保存结果
    result_df.to_csv(output_file_path, index=False)

    print(f"结果已保存至: {output_file_path}")
#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_UK_2024-07-26','LAPASA','2024-07-26','UK')
