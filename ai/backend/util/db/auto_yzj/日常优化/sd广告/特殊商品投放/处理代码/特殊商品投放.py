# filename: 手动_特殊关键词_v1_1_ES_2024-06-20.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 加载数据
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\特殊商品投放\预处理.csv'
    file_name = "SD_特殊商品sd投放" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)
    if version == 1:
        # 过滤符合条件的广告组
        filtered_data = data[(data['total_sales_15d'] == 0) & (data['total_clicks_7d'] <= 12)]

        # 对关键词的竞价增加0.02
        filtered_data['new_keywordBid'] = filtered_data['keywordBid'] + 0.02

        # 添加操作竞价原因
        filtered_data['reason'] = '广告组最近15天的总销售额为0，且广告组里的所有关键词的最近7天的总点击次数小于等于12'

        # 选择需要输出的字段
        output_columns = [
            'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d',
            'keyword', 'matchType', 'keywordBid', 'targetingId', 'new_keywordBid', 'reason'
        ]
        output_data = filtered_data[output_columns]

        # 指定输出文件路径
        output_data.to_csv(output_file_path, index=False)

        print(f"结果已保存到 {output_file_path}")
    elif version == 2:

        # 定义规则
        def apply_rules(row):
            if row['total_sales_7d'] == 0 and row['total_cost_7d'] <= 3:
                return row['bid'] + 0.9, '定义一', 0.9
            else:
                return row['bid'], '', ''

        # 应用规则并创建新的列
        data[['New_Bid', 'reason', 'bid_adjust']] = data.apply(apply_rules, axis=1, result_type='expand')

        # 筛选出符合条件的数据
        filtered_data = data[data['reason'] != '']

        # 选择需要的列
        filtered_data = filtered_data[[
            'campaignName', 'campaignId', 'adGroupName', 'total_sales_7d', 'total_cost_7d',
            'targetingText', 'bid', 'keywordId', 'New_Bid', 'reason', 'bid_adjust'
        ]]
        filtered_data.replace({np.nan: None}, inplace=True)
        api = DbNewSpTools(brand,country)
        for index, row in filtered_data.iterrows():
            api.create_product_targets_info(country, brand, '日常优化', 'SD_特殊', row['targetingText'],
                                                row['keywordId'],
                                                row['campaignName'], row['adGroupName'], None,row['bid'],
                                                row['New_Bid'], None, None,
                                                None, None,None,None,
                                                None, None, row['total_sales_7d'], row['total_cost_7d'],
                                                None, None, None, row['reason'], row['bid_adjust'],
                                                cur_time,
                                                datetime.now(), 0)
        # 保存结果到新CSV文件
        filtered_data.to_csv(output_file_path, index=False)
        print("关键词竞价调整结果已保存至CSV文件.")


# main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-22',
#      'LAPASA', '2024-07-22', 'US')

