import json
import traceback
import logging
from datetime import datetime, timedelta
import time
import numpy as np

from util.InserOnlineData import ProcessShowData
from db_tool.tools_db import AmazonMysqlRagUitl
from db_tool.ads_db import AmazonMysqlRagUitl as api


def default_dump(obj):
    """Convert numpy classes to JSON serializable objects."""
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj

def get_request_data(CountryCode, StartDate, report_type, all_table, data_id, brand):
    if CountryCode == 'US':
        ContinentCode = 'NA'
    elif CountryCode == 'JP':
        ContinentCode = 'FE'
    else:
        ContinentCode = 'EU'
    if brand == 'LAPASA':
        UID = "1"
    elif brand == 'DELOMO':
        UID = "4"
    elif brand == 'OutdoorMaster':
        UID = "3"
    else:
        UID = None
    try:
        add_data = {
            "UID": UID,
            "ContinentCode": ContinentCode,
            "CountryCode": CountryCode,
            "DataType": report_type,
            "StartDate": StartDate,
            "EndDate": StartDate,
            "ShowData": json.dumps(all_table,ensure_ascii=False, default=default_dump),
            "Other": ""
        }

        # logger.info(f"发送数据请求: {add_data}")
        if data_id > 0:
            print(f"发送数据请求: 正在更新 {report_type} 的ID为 {id}")
            add_data['ID'] = data_id
            res, data = ProcessShowData.update(add_data)
        else:
            print(f"发送数据请求: 正在插入 {report_type} 的ID为 {id}")
            # 发送请求
            res, data = ProcessShowData.insert(add_data)
        if res:
            print("请求成功: 操作成功")
            print(data)
            data = data['data']
        else:
            print("请求失败: 操作失败")
            data = 0
        return data
    except Exception as e:
        print(f"请求失败: {e}")
        traceback.format_exc()
        return 0

# res = get_request_data("FR","2024-06-25","AIADADD",
# [
# {"x":"2024-06-25","type":"扫描广告活动","y":682},
# {"x":"2024-06-25","type":"新建广告活动","y":12}
# ],0)
# print(res)

# res = get_request_data("FR","2024-06-25","AIADOPTIM",
# [
#   { "date": "2024-06-25", "type": "预算优化", "value": 13 },
# { "date": "2024-06-25", "type": "广告位置优化", "value": 24 },
# { "date": "2024-06-25", "type": "关键词优化", "value": 42 },
# { "date": "2024-06-25", "type": "SKU优化", "value": 74 }
# ],0)
# print(res)

def get_data(market,brand):
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    cur_time = yesterday.strftime('%Y-%m-%d')
    cur_time = '2024-07-16'
    api2 = api(brand)
    sp_count = api2.get_scan_campaign_sp(market, cur_time)
    sd_count = api2.get_scan_campaign_sd(market, cur_time)
    sb_count = api2.get_scan_campaign_sb(market, cur_time)
    all_count = sp_count + sd_count + sb_count
    api1 = AmazonMysqlRagUitl(brand)
    new_create = api1.get_new_create_campaign(market, cur_time)
    budget = api1.get_update_budget(market, cur_time)
    targeting_group = api1.get_update_targeting_group(market, cur_time)
    keyword = api1.get_update_keyword(market, cur_time)
    SKU = api1.get_update_sku(market, cur_time)
    # print(budget)
    # print(targeting_group)
    # print(keyword)
    # print(SKU)
    all_table = [[cur_time,  "预算优化", budget],
                 [cur_time, "广告位置优化", targeting_group],
                 [cur_time, "关键词优化", keyword],
                 [cur_time, "SKU优化", SKU]
                 ]
    #print(json.dumps(all_table,ensure_ascii=False, default=default_dump))
    get_request_data(market,cur_time,"D-Trim",all_table,0,brand)
    all_table2 = [[cur_time,"量化广告分析",all_count],
                  [cur_time,"新建广告活动",new_create]
                  ]
    get_request_data(market,cur_time,"D-CALL",all_table2,0,brand)
    #print(json.dumps(all_table2,ensure_ascii=False, default=default_dump))

# time.sleep(60 * 60 * 4)
brands_and_countries = {
    'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK", "JP"],
    'DELOMO': ['IT', 'ES', 'DE', 'FR'],
    'OutdoorMaster': ['IT', 'ES', 'FR']
}
while True:
    for brand, countries in brands_and_countries.items():
        for country in countries:
            get_data(country, brand)
    print('done')
    time.sleep(60 * 60 * 24)
#4-1 2-12
