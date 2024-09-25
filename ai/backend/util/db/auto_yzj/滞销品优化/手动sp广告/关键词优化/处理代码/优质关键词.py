# filename: process_keywords.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


# 定义条件函数
def get_raise_amount(row, version=2):
    ACOS_3d = row['ACOS_3d']
    ACOS_7d = row['ACOS_7d']
    ACOS_30d = row['ACOS_30d']
    orders_1m = row['ORDER_1m']
    if version == 1:
        if 0 < ACOS_7d <= 0.2 and 0 < ACOS_30d <= 0.2 and orders_1m >= 2:
            return 0.05, '定义一'
        elif 0 < ACOS_7d <= 0.2 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
            return 0.03, '定义二'
        elif 0.2 < ACOS_7d <= 0.24 and 0 < ACOS_30d <= 0.2 and orders_1m >= 2:
            return 0.04, '定义三'
        elif 0.2 < ACOS_7d <= 0.24 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
            return 0.02, '定义四'
        elif 0.24 < ACOS_7d <= 0.27 and 0 < ACOS_30d <= 0.2 and orders_1m >= 2:
            return 0.02, '定义五'
        elif 0.24 < ACOS_7d <= 0.27 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
            return 0.01, '定义六'
        else:
            return 0, ''
    elif version == 2:
        if 0 < ACOS_3d <= 0.24 and 0 < ACOS_7d <= 0.2 and 0 < ACOS_30d <= 0.2 and orders_1m >= 2:
            return 0.05, '定义一'
        elif 0 < ACOS_3d <= 0.24 and 0 < ACOS_7d <= 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
            return 0.03, '定义二'
        elif 0 < ACOS_3d <= 0.24 and 0.24 >= ACOS_7d > 0.2 >= ACOS_30d > 0 and orders_1m >= 2:
            return 0.04, '定义三'
        elif 0 < ACOS_3d <= 0.24 and 0.2 < ACOS_7d <= 0.24 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
            return 0.02, '定义四'
        elif 0 < ACOS_3d <= 0.24 < ACOS_7d <= 0.27 and 0 < ACOS_30d <= 0.2 and orders_1m >= 2:
            return 0.02, '定义五'
        elif 0 < ACOS_3d <= 0.24 < ACOS_7d <= 0.27 and 0.2 < ACOS_30d <= 0.27 and orders_1m >= 2:
            return 0.01, '定义六'
        else:
            return 0, ''

def main(path, brand, cur_time, country):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\预处理.csv'
    file_name = "手动_优质关键词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        df = pd.read_csv(file_path)
    else:
        # 如果文件不存在或为空，则创建一个空的DataFrame
        df = pd.DataFrame(columns=['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
                                   'keywordBid', 'new_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_7d',
                                   'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'bid_adjust', 'reason'])
        df.to_csv(output_file_path, index=False)
        print(f"结果已保存至 {output_file_path}")
        return

    if df.empty:
        df.to_csv(output_file_path, index=False)
        print(f"结果已保存至 {output_file_path}")
        return

    # 应用条件函数
    df['bid_adjust'], df['reason'] = zip(*df.apply(get_raise_amount, axis=1))

    # 筛选出符合条件的关键词并计算新竞价
    df['new_keywordBid'] = df['keywordBid'] + df['bid_adjust']
    result_df = df[df['bid_adjust'] > 0][[
        'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
        'keywordBid', 'new_keywordBid', 'ACOS_3d',
        'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'bid_adjust', 'reason'
    ]]
    result_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in result_df.iterrows():
        api.create_keyword_info(country,brand,'滞销品优化','手动_优质',row['keyword'],row['keywordId'],row['campaignName'],row['adGroupName'],row['matchType'],row['keywordBid'],row['new_keywordBid'],row['ACOS_30d'],row['ORDER_1m'],None,None,None,None,row['ACOS_7d'],None,None,None,row['ACOS_3d'],None,None,row['reason'],row['bid_adjust'],cur_time,datetime.now(),0)

    # 输出结果到指定文件
    result_df.to_csv(output_file_path, index=False)

    print(f"结果已保存至 {output_file_path}")


#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/滞销品优化/输出结果/LAPASA_US_2024-07-17','LAPASA','2024-07-17','US')
