# filename: 表现较差的广告位更新竞价.py

import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 读取CSV数据
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
    file_name = "自动_劣质广告位" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)
    if version == 1:
        # 定义一个新的列用于存储新的竞价
        data['new_bid'] = data['bid']

        # --- 定义一 ---#
        # 最近7天的总sales为0，但最近7天的总点击数大0的广告位竞价变为0
        data.loc[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0), 'new_bid'] = 0

        # --- 定义二 ---#
        # 找出同一广告活动中满足条件的广告位，并降低指定竞价
        for campaign_id in data['campaignId'].unique():
            campaign_data = data[data['campaignId'] == campaign_id]
            if len(campaign_data) == 3:
                acos_7d = campaign_data['ACOS_7d']
                if acos_7d.between(24, 50).all() and (acos_7d.max() - acos_7d.min() >= 0.2):
                    max_acos_index = acos_7d.idxmax()
                    data.loc[max_acos_index, 'new_bid'] = max(0, data.loc[max_acos_index, 'new_bid'] - 3)

        # --- 定义三 ---#
        # 最近7天的平均ACOS值大于等于50%的广告位竞价变为0
        data.loc[data['ACOS_7d'] >= 50, 'new_bid'] = 0

        # 添加竞价操作的具体原因的列
        data['reason'] = ''

        # 填写reason
        data.loc[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0), 'reason'] = '定义一'

        for campaign_id in data['campaignId'].unique():
            campaign_data = data[data['campaignId'] == campaign_id]
            if len(campaign_data) == 3:
                acos_7d = campaign_data['ACOS_7d']
                if acos_7d.between(24, 50).all() and (acos_7d.max() - acos_7d.min() >= 0.2):
                    max_acos_index = acos_7d.idxmax()
                    data.loc[max_acos_index, 'reason'] = '定义二'

        data.loc[data['ACOS_7d'] >= 50, 'reason'] = '定义三'

        # 添加竞价操作的具体原因的列
        data['bid_adjust'] = ''

        # 填写reason
        data.loc[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0), 'bid_adjust'] = 0

        for campaign_id in data['campaignId'].unique():
            campaign_data = data[data['campaignId'] == campaign_id]
            if len(campaign_data) == 3:
                acos_7d = campaign_data['ACOS_7d']
                if acos_7d.between(24, 50).all() and (acos_7d.max() - acos_7d.min() >= 0.2):
                    max_acos_index = acos_7d.idxmax()
                    data.loc[max_acos_index, 'bid_adjust'] = -3

        data.loc[data['ACOS_7d'] >= 50, 'bid_adjust'] = 0

        # 过滤需要输出的列
        output_columns = [
            'campaignName',
            'campaignId',
            'placementClassification',
            'ACOS_7d',
            'total_sales14d_7d',
            'total_clicks_7d',
            'bid',
            'new_bid',
            'reason',
            'bid_adjust'
        ]

        result_data = data[data['reason'] != ''][output_columns]
    elif version == 2:
        results = []
        for index, row in data.iterrows():
            campaignId = row['campaignId']
            campaignName = row['campaignName']
            placementClassification = row['placementClassification']
            ACOS_7d = row['ACOS_7d']
            ACOS_3d = row['ACOS_3d']
            total_sales14d_7d = row['total_sales14d_7d']
            total_clicks_7d = row['total_clicks_7d']
            bid = row['bid']
            if total_sales14d_7d == 0 and total_clicks_7d > 15:
                reason = '定义一'
                bid_adjust = 0
                new_bid = 0
                results.append([campaignName, campaignId, placementClassification, ACOS_7d, ACOS_3d, total_sales14d_7d, total_clicks_7d,
                                bid, new_bid, reason, bid_adjust])

            elif ACOS_7d > 0.5:
                reason = '定义二'
                bid_adjust = 0
                new_bid = 0
                results.append([campaignName, campaignId, placementClassification, ACOS_7d, ACOS_3d, total_sales14d_7d,
                                total_clicks_7d,
                                bid, new_bid, reason, bid_adjust])
        columns = ['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_sales14d_7d', 'total_clicks_7d', 'bid', 'new_bid', 'reason', 'bid_adjust']
        result_data = pd.DataFrame(results, columns=columns)
    api2 = AmazonMysqlRagUitl(brand,country)
    campaignIds_to_remove, placements_to_remove = api2.get_operated_campaign_placement(country, cur_time)
    if campaignIds_to_remove and placements_to_remove:
        campaignIds_to_remove = [int(campaign_id) for campaign_id in campaignIds_to_remove]
        # 创建布尔掩码，筛选出需要保留的行
        mask = ~(result_data['campaignId'].isin(campaignIds_to_remove) & result_data['placementClassification'].isin(
            placements_to_remove))
        # 根据掩码过滤数据框
        result_data = result_data[mask]
    result_data.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in result_data.iterrows():
        api.create_campaign_placement_info(country, brand, '日常优化', '手动_劣质', row['campaignName'],
                                           row['campaignId'], row['placementClassification'], row['bid'],
                                           row['new_bid'], row['ACOS_7d'], row['total_clicks_7d'],
                                           row['total_sales14d_7d'],
                                           None, None, None, row['reason'],row['bid_adjust'], cur_time,
                                           datetime.now(), 0)

    # 输出到新的CSV文件
    result_data.to_csv(output_file_path, index=False)

    print(f"处理完成，结果已保存到{output_file_path}")
