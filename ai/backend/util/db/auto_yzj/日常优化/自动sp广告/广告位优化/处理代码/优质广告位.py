# filename: process_ad_placements.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # Load the CSV file into a DataFrame
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
    file_name = "自动_优质广告位" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)
    if version == 1:
        # 定义条件
        condition_7d = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
        condition_3d = (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24)

        # 分组按广告活动
        grouped = data.groupby('campaignId')

        result = []
        for name, group in grouped:
            # 最近7天的最小ACOS广告位，排除NaN值来寻找最小值
            group_7d_clean = group.dropna(subset=['ACOS_7d'])
            group_3d_clean = group.dropna(subset=['ACOS_3d'])

            if group_7d_clean.empty or group_3d_clean.empty:
                continue

            min_acos_7d_row = group_7d_clean.loc[group_7d_clean['ACOS_7d'].idxmin()]
            min_acos_3d_row = group_3d_clean.loc[group_3d_clean['ACOS_3d'].idxmin()]

            if (condition_7d[min_acos_7d_row.name] and condition_3d[min_acos_3d_row.name]):
                max_clicks_7d = group['total_clicks_7d'].max()
                max_clicks_3d = group['total_clicks_3d'].max()

                if (min_acos_7d_row['total_clicks_7d'] != max_clicks_7d and
                    min_acos_3d_row['total_clicks_3d'] != max_clicks_3d):
                    reason = "ACOS符合条件，平均ACOS值最低, 并且最近总点击次数不是最大"
                    new_bid = min(min_acos_7d_row['bid'] + 5, 50)

                    # 把需要的数据收集起来
                    result.append({
                        'campaignName': min_acos_7d_row['campaignName'],
                        'campaignId': name,
                        'placementClassification': min_acos_7d_row['placementClassification'],
                        'ACOS_7d': min_acos_7d_row['ACOS_7d'],
                        'ACOS_3d': min_acos_7d_row['ACOS_3d'],
                        'total_clicks_7d': min_acos_7d_row['total_clicks_7d'],
                        'total_clicks_3d': min_acos_7d_row['total_clicks_3d'],
                        'bid': min_acos_7d_row['bid'],
                        'new_bid': new_bid,
                        'reason': reason
                    })

        # 转换为DataFrame
        result_df = pd.DataFrame(result)
    elif version == 2:
        results = []
        for index, row in data.iterrows():
            campaignId = row['campaignId']
            campaignName = row['campaignName']
            placementClassification = row['placementClassification']
            ACOS_7d = row['ACOS_7d']
            ACOS_3d = row['ACOS_3d']
            total_sales14d_3d = row['total_clicks_7d']
            total_cost_3d = row['total_clicks_3d']
            bid = row['bid']
            if 0 < ACOS_7d < 0.24 and 0 < ACOS_3d < 0.24:
                reason = '定义一'
                new_bid = min(50, row['bid'] + 5)
                results.append([campaignName, campaignId, placementClassification, ACOS_7d, ACOS_3d, total_sales14d_3d, total_cost_3d,
                                bid, new_bid, reason])

            elif 0 < ACOS_7d < 0.24 and total_sales14d_3d == 0 and total_cost_3d < 5:
                reason = '定义二'
                new_bid = min(50, row['bid'] + 5)
                results.append([campaignName, campaignId, placementClassification, ACOS_7d, ACOS_3d, total_sales14d_3d,
                                total_cost_3d,
                                bid, new_bid, reason])
        columns = ['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_sales14d_3d', 'total_cost_3d', 'bid', 'new_bid', 'reason']
        result_df = pd.DataFrame(results, columns=columns)

    api2 = AmazonMysqlRagUitl(brand)
    campaignIds_to_remove, placements_to_remove = api2.get_operated_campaign_placement(country, cur_time)
    if campaignIds_to_remove and placements_to_remove:
        campaignIds_to_remove = [int(campaign_id) for campaign_id in campaignIds_to_remove]
        # 创建布尔掩码，筛选出需要保留的行
        mask = ~(result_df['campaignId'].isin(campaignIds_to_remove) & result_df['placementClassification'].isin(placements_to_remove))
        # 根据掩码过滤数据框
        result_df = result_df[mask]
    result_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in result_df.iterrows():
        api.create_campaign_placement_info(country,brand,'日常优化','自动_优质',row['campaignName'],row['campaignId'],row['placementClassification'],row['bid'],row['new_bid'],row['ACOS_7d'],None,None,row['ACOS_3d'],row['total_sales14d_3d'],row['total_cost_3d'],row['reason'],cur_time,datetime.now(),0)


    # Save the output to a new CSV file
    result_df.to_csv(output_file_path, index=False)

    print(f"Filtered placements data saved to {output_file_path}")
