# filename: filter_search_terms.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
    file_name = "手动_劣质_ASIN_搜索词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    df = pd.read_csv(file_path)

    # 定义筛选条件
    condition_1 = (df['total_clicks_30d'] > 13) & (df['total_cost_30d'] > 7) & (df['ORDER_1m'] == 0)
    condition_2 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0) & (df['total_cost_7d'] > 5)

    # 筛选符合条件的数据
    filtered_df = df[condition_1 | condition_2].copy()

    # 添加原因列
    filtered_df['reason'] = ''
    filtered_df.loc[condition_1, 'reason'] = '定义一'
    filtered_df.loc[condition_2, 'reason'] = '定义二'

    # 筛选所需列
    required_columns = [
        'campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'ORDER_1m', 'total_clicks_30d', 'total_cost_30d',
        'ORDER_7d', 'total_clicks_7d', 'total_cost_7d', 'searchTerm', 'reason'
    ]
    result_df = filtered_df[required_columns]
    result_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in result_df.iterrows():
        api.create_product_targets_search_term_info(country, brand, '日常优化', '手动_劣质', row['campaignName'],row['campaignId'],
                               row['adGroupName'], row['adGroupId'], None,
                               row['ORDER_1m'],row['total_clicks_30d'],row['total_cost_30d'], None, row['ORDER_7d'],row['total_clicks_7d'], None,
                               row['total_cost_7d'], row['searchTerm'], row['reason'], cur_time,
                               datetime.now(), 0)
    # 输出结果到CSV文件
    result_df.to_csv(output_file_path, index=False)

    print(f"结果已保存到 {output_file_path}")


#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-15','LAPASA','2024-07-15','US')
