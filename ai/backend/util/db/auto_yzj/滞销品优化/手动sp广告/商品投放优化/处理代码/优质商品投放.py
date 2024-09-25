# filename: 提价策略.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version: int = 1):
    # 定义提价策略的函数
    def adjust_bid(df):
        results = []

        for _, row in df.iterrows():
            bid_increase = 0
            reason = ""

            if (0 < row['ACOS_7d'] <= 0.2 and
                0 < row['ACOS_30d'] <= 0.2 and
                row['ORDER_1m'] >= 2 and
                0 < row['ACOS_3d'] <= 0.24):
                bid_increase = 0.05
                reason = "定义一"
            elif (0 < row['ACOS_7d'] <= 0.2 and
                  0.2 < row['ACOS_30d'] <= 0.27 and
                  row['ORDER_1m'] >= 2 and
                  0 < row['ACOS_3d'] <= 0.24):
                bid_increase = 0.03
                reason = "定义二"
            elif (0.1 < row['ACOS_7d'] <= 0.24 and
                  0 < row['ACOS_30d'] <= 0.2 and
                  row['ORDER_1m'] >= 2 and
                  0 < row['ACOS_3d'] <= 0.24):
                bid_increase = 0.04
                reason = "定义三"
            elif (0.2 < row['ACOS_7d'] <= 0.24 and
                  0.2 < row['ACOS_30d'] <= 0.27 and
                  row['ORDER_1m'] >= 2 and
                  0 < row['ACOS_3d'] <= 0.24):
                bid_increase = 0.02
                reason = "定义四"
            elif (0.24 < row['ACOS_7d'] <= 0.27 and
                  0 < row['ACOS_30d'] <= 0.2 and
                  row['ORDER_1m'] >= 2 and
                  0 < row['ACOS_3d'] <= 0.24):
                bid_increase = 0.02
                reason = "定义五"
            elif (0.24 < row['ACOS_7d'] <= 0.27 and
                  0.2 < row['ACOS_30d'] <= 0.27 and
                  row['ORDER_1m'] >= 2 and
                  0 < row['ACOS_3d'] <= 0.24):
                bid_increase = 0.01
                reason = "定义六"

            if bid_increase > 0:
                new_keyword_bid = row['keywordBid'] + bid_increase
                results.append([
                    row['keyword'],
                    row['keywordId'],
                    row['campaignName'],
                    row['adGroupName'],
                    row['matchType'],
                    row['keywordBid'],
                    new_keyword_bid,
                    row['targeting'],
                    row['ACOS_30d'],
                    row['ORDER_1m'],
                    row['ACOS_7d'],
                    row['ACOS_3d'],
                    bid_increase,
                    reason
                ])

        return results

    # 读取数据
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\商品投放优化\预处理.csv'
    file_name = "手动_ASIN_优质商品投放" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    df = pd.read_csv(file_path)

    # 调整竞价
    adjusted_bids = adjust_bid(df)

    # 创建结果的DataFrame
    columns = [
        'keyword', 'keywordId', 'campaignName', 'adGroupName',
        'matchType', 'keywordBid', 'New_keywordBid', 'targeting',
        'ACOS_30d', 'ORDER_1m', 'ACOS_7d', 'ACOS_3d',
        'bid_adjust', 'reason'
    ]
    output_df = pd.DataFrame(adjusted_bids, columns=columns)
    output_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in output_df.iterrows():
        api.create_product_targets_info(country,brand,'滞销品优化','手动_优质',row['keyword'],row['keywordId'],row['campaignName'],row['adGroupName'],row['matchType'],row['keywordBid'],row['New_keywordBid'],row['ACOS_30d'],row['ORDER_1m'],None,None,None,None,row['ACOS_7d'],None,None,None,row['ACOS_3d'],None,None,row['reason'],row['bid_adjust'],cur_time,datetime.now(),0)
    # 输出结果到指定文件
    # 保存结果
    output_df.to_csv(output_file_path, index=False)

    print(f'Results saved to {output_file_path}')

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/滞销品优化/输出结果/LAPASA_US_2024-07-18','LAPASA','2024-07-18','US')
