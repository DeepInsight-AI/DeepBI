from MySQLQueryExecutor import MySQLQueryExecutor
from backend.util.db.auto_yzj.日常优化.Preprocessing import AmazonMysqlRagUitl
from ai.backend.util.db.auto_yzj.utils.find import find_files,find_file_by_name
import os

def preprocess_data(cur_time: str, country: str):
    # 实例化类
    amr = AmazonMysqlRagUitl()
    amr.preprocessing_sku(country, cur_time)
    amr.preprocessing_keyword(country, cur_time)
    amr.preprocessing_targeting_group(country, cur_time)
    amr.preprocessing_search_term(country, cur_time)
    amr.preprocessing_spkeyword(country, cur_time)
    amr.preprocessing_budget(country, cur_time)

    amr.preprocessing_sku_auto(country, cur_time)
    amr.preprocessing_automatic_targeting_auto(country, cur_time)
    amr.preprocessing_sp_automatic_targeting_auto(country, cur_time)
    amr.preprocessing_search_term_auto(country, cur_time)
    amr.preprocessing_targeting_group_auto(country, cur_time)
    amr.preprocessing_budget_auto(country, cur_time)

    # amr.preprocessing_campaign_anomaly_detection(country, cur_time)
    # amr.preprocessing_targeting_group_anomaly_detection(country, cur_time)

    # 替换为你的项目目录路径
    # csv_files = find_file_by_name(directory='./日常优化/', filename='预处理1.csv')
    # print(csv_files)

def extract_campaign_ids(file_paths):
    campaign_ids = set()
    for file_path in file_paths:
        with open(file_path, 'r', encoding='utf-8') as file:
            # 假设csv文件的第一行是标题行，其中campaignId在第一列
            next(file)  # 跳过标题行
            for line in file:
                campaign_id = line.split(',')[0]  # 假设使用逗号分隔csv文件
                campaign_ids.add(campaign_id.strip())  # 去除首尾空格并添加到集合中
    return campaign_ids


def preprocess_sp_data(cur_time: str, country: str):
    output_path = country + '_' + cur_time + '.csv'
    mds = find_files(directory='./日常优化/异常定位检测/', suffix=output_path)
    print(mds)
    campaign_ids = tuple(extract_campaign_ids(mds))
    # 打印并集并去重后的campaignId
    print(campaign_ids)
    amr = AmazonMysqlRagUitl()
    amr.preprocessing_sku_anomaly_detection(country, cur_time, campaign_ids)
    amr.preprocessing_targeting_anomaly_detection(country, cur_time, campaign_ids)



