import json
import os
import traceback
import logging
from datetime import datetime, timedelta
import time
import numpy as np
import yaml

from ai.backend.util.db.auto_process.summary.util.InserOnlineData import ProcessShowData
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from ai.backend.util.db.auto_process.summary.db_tool.ads_db import AmazonMysqlRagUitl as api
from ai.backend.util.db.configuration.marketplaces import get_continent_code
from ai.backend.util.db.configuration.path import get_config_path


def load_uid(brand, country=None):
    # 从 JSON 文件加载数据库信息
    Brand_path = os.path.join(get_config_path(), 'Brand.yml')
    with open(Brand_path, 'r') as file:
        Brand_data = yaml.safe_load(file)

    brand_info = Brand_data.get(brand, {})
    if country:
        country_info = brand_info.get(country, {})
        return country_info.get('UID', brand_info.get('default', {}).get('UID'))
    return brand_info.get('UID', brand_info.get('default', {}).get('UID'))


# def default_dump(obj):
#     """Convert numpy classes to JSON serializable objects."""
#     if isinstance(obj, (np.integer, np.floating, np.bool_)):
#         return obj.item()
#     elif isinstance(obj, np.ndarray):
#         return obj.tolist()
#     else:
#         return obj

def get_request_data(CountryCode, StartDate, report_type, all_table, data_id, brand, ID=None):
    try:

        if data_id > 0:
            add_data = {
                "ID": str(ID),
                "UID": str(load_uid(brand, CountryCode)),
                "ContinentCode": get_continent_code(CountryCode),
                "CountryCode": CountryCode,
                "DataType": report_type,
                "StartDate": StartDate,
                "EndDate": StartDate,
                "ShowData": json.dumps(all_table)
            }
            print(add_data)
            print(f"发送数据请求: 正在更新 {report_type} 的ID为 {ID}")
            res, data = ProcessShowData.update(add_data)
        else:
            add_data = {
                "UID": load_uid(brand, CountryCode),
                "ContinentCode": get_continent_code(CountryCode),
                "CountryCode": CountryCode,
                "DataType": report_type,
                "StartDate": StartDate,
                "EndDate": StartDate,
                "ShowData": json.dumps(all_table)
            }
            #print(add_data)
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
storage_path = os.path.join(get_config_path(), 'brand_storage.json')


def load_storage():
    """加载存储文件中的品牌和 ID 映射"""
    if not os.path.exists(storage_path):
        # 如果存储文件不存在，创建一个空文件
        with open(storage_path, 'w', encoding='utf-8') as f:
            json.dump({}, f)

    with open(storage_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_storage(storage):
    """保存品牌和 ID 映射到存储文件"""
    with open(storage_path, 'w', encoding='utf-8') as f:
        json.dump(storage, f, ensure_ascii=False, indent=4)

def create_summarize_data():
    tesk_path = os.path.join(get_config_path(), 'tesk.json')
    with open(tesk_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    cur_time = yesterday.strftime('%Y-%m-%d')

    storage = load_storage()

    for original_brand, items in data.items():
        # 统一品牌映射
        brand = 'Gvyugke' if original_brand == 'TEST' else ('DELOMO' if original_brand == 'TEST2' else original_brand)

        all_res = []
        print(f"品牌: {brand}")

        for item in items:
            if item.get("state") == 0:
                df = api(brand, item['country']).get_summarize_data_info_one_country(item)
            elif item.get("state") == 1:
                df = api(brand, item['config']).get_summarize_data_info_summarize_country(item)

            for index, row in df.iterrows():
                res = [str(row['国家']), str(row['总销售日期']), str(row['广告总销售额']), str(row['广告总花费']),
                       str(row['广告总ACOS']), str(row['DeepBI计划花费']),
                       str(row['DeepBI计划销量']), str(row['新开计划acos']), str(row['新开计划销量占比']),
                       str(row['旧计划销售额']), str(row['旧计划花费']),
                       str(row['旧计划acos']), str(row['旧计划销量占比']), str(row['总销售额']), str(row['广告销售额']),
                       str(row['自然销售额']),
                       str(row['自然销售额比例'])]
                all_res.append(res)
                cleaned_res = [[item if item not in ('nan', 'None') else '0' for item in sublist] for sublist in all_res]

        # 执行插入操作并存储返回的 ID
        if original_brand in storage:
            stored_id = storage[original_brand]
            print(f"使用存储的ID进行更新: {stored_id}")
            print(all_res)
            get_request_data(item['config'], cur_time, "B-TABLE", cleaned_res, 1, brand, stored_id)
            continue
        else:
            res = get_request_data(item['config'], cur_time, "B-TABLE", cleaned_res, 0, brand)
            print(f"插入操作返回的 ID: {res}")
        if res:
            storage[original_brand] = res
            print(storage)
            save_storage(storage)


def get_data(market, brand):
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    cur_time = yesterday.strftime('%Y-%m-%d')
    # cur_time = '2024-08-12'
    sp_count = api(brand, market).get_scan_campaign_sp(market, cur_time)
    sd_count = api(brand, market).get_scan_campaign_sd(market, cur_time)
    sb_count = api(brand, market).get_scan_campaign_sb(market, cur_time)
    all_count = sp_count + sd_count + sb_count
    new_create = AmazonMysqlRagUitl(brand, market).get_new_create_campaign(market, cur_time)
    budget = AmazonMysqlRagUitl(brand, market).get_update_budget(market, cur_time)
    targeting_group = AmazonMysqlRagUitl(brand, market).get_update_targeting_group(market, cur_time)
    keyword = AmazonMysqlRagUitl(brand, market).get_update_keyword(market, cur_time)
    SKU = AmazonMysqlRagUitl(brand, market).get_update_sku(market, cur_time)
    # print(budget)
    # print(targeting_group)
    # print(keyword)
    # print(SKU)
    all_table = [[cur_time, "预算优化", budget],
                 [cur_time, "广告位置优化", targeting_group],
                 [cur_time, "关键词优化", keyword],
                 [cur_time, "SKU优化", SKU]
                 ]
    # print(json.dumps(all_table,ensure_ascii=False, default=default_dump))
    get_request_data(market, cur_time, "D-Trim", all_table, 0, brand)
    all_table2 = [[cur_time, "量化广告分析", all_count],
                  [cur_time, "新建广告活动", new_create]
                  ]
    get_request_data(market, cur_time, "D-CALL", all_table2, 0, brand)
    # print(json.dumps(all_table2,ensure_ascii=False, default=default_dump))


def get_data_temporary(market, brand):
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    cur_time = yesterday.strftime('%Y-%m-%d')
    # cur_time = '2024-08-25'
    # 以下是您的 API 调用和数据处理逻辑
    sp_count = api(brand, market).get_scan_campaign_sp(market, cur_time)
    sd_count = api(brand, market).get_scan_campaign_sd(market, cur_time)
    sb_count = api(brand, market).get_scan_campaign_sb(market, cur_time)
    all_count = sp_count + sd_count + sb_count


    new_create = AmazonMysqlRagUitl(brand, market).get_new_create_campaign(market, cur_time)
    budget = AmazonMysqlRagUitl(brand, market).get_update_budget(market, cur_time)
    targeting_group = AmazonMysqlRagUitl(brand, market).get_update_targeting_group(market, cur_time)
    keyword = AmazonMysqlRagUitl(brand, market).get_update_keyword(market, cur_time)
    keyword1 = api(brand, market).get_scan_keyword(market, cur_time)
    SKU = AmazonMysqlRagUitl(brand, market).get_update_sku(market, cur_time)
    SKU1 = api(brand, market).get_scan_sku(market, cur_time)

    # 构建表格数据
    all_table = [[str(cur_time), "预算优化", str(budget)],
                 [str(cur_time), "广告位置优化", str(targeting_group)],
                 [str(cur_time), "关键词优化", str((keyword or 0) + (keyword1 or 0))],
                 [str(cur_time), "SKU优化", str((SKU or 0) + (SKU1 or 0))]
                 ]
    print(all_table)
    # 发送请求数据
    get_request_data(market, cur_time, "D-Trim", all_table, 0, brand)

    all_table2 = [[str(cur_time), "量化广告分析", str(all_count)],
                  [str(cur_time), "新建广告活动", str(new_create)]
                  ]

    # 如果需要，发送其他请求数据
    get_request_data(market, cur_time, "D-CALL", all_table2, 0, brand)


def get_data_temporary_period(market, brand):
    start_date = datetime.strptime('2024-08-02', '%Y-%m-%d')
    today = datetime.today()
    yesterday = today - timedelta(days=1)

    # 循环遍历日期范围
    current_date = start_date
    while current_date <= yesterday:
        cur_time = current_date.strftime('%Y-%m-%d')

        # 以下是您的 API 调用和数据处理逻辑
        api2 = api(brand, market)
        sp_count = api2.get_scan_campaign_sp(market, cur_time)
        sd_count = api2.get_scan_campaign_sd(market, cur_time)
        sb_count = api2.get_scan_campaign_sb(market, cur_time)
        all_count = sp_count + sd_count + sb_count

        api1 = AmazonMysqlRagUitl(brand, market)
        new_create = api1.get_new_create_campaign(market, cur_time)
        budget = api1.get_update_budget(market, cur_time)
        targeting_group = api1.get_update_targeting_group(market, cur_time)
        keyword = api1.get_update_keyword(market, cur_time)
        keyword1 = api2.get_scan_keyword(market, cur_time)
        SKU = api1.get_update_sku(market, cur_time)
        SKU1 = api2.get_scan_sku(market, cur_time)

        # 构建表格数据
        all_table = [[cur_time, "预算优化", budget],
                     [cur_time, "广告位置优化", targeting_group],
                     [cur_time, "关键词优化", (keyword or 0) + (keyword1 or 0)],
                     [cur_time, "SKU优化", (SKU or 0) + (SKU1 or 0)]
                     ]

        # 发送请求数据
        get_request_data(market, cur_time, "D-Trim", all_table, 0, brand)

        all_table2 = [[cur_time, "量化广告分析", all_count],
                      [cur_time, "新建广告活动", new_create]
                      ]

        # 如果需要，发送其他请求数据
        get_request_data(market, cur_time, "D-CALL", all_table2, 0, brand)

        # 准备下一次循环
        current_date += timedelta(days=1)


def get_data_period(market, brand):
    today = datetime.today()
    for i in range(30):
        cur_time = (today - timedelta(days=i + 7)).strftime('%Y-%m-%d')
        # yesterday = today - timedelta(days=1)
        # cur_time = yesterday.strftime('%Y-%m-%d')
        # cur_time = '2024-07-27'
        api2 = api(brand, market)
        sp_count = api2.get_scan_campaign_sp(market, cur_time)
        sd_count = api2.get_scan_campaign_sd(market, cur_time)
        sb_count = api2.get_scan_campaign_sb(market, cur_time)
        all_count = sp_count + sd_count + sb_count
        api1 = AmazonMysqlRagUitl(brand, market)
        new_create = api1.get_new_create_campaign(market, cur_time)
        budget = api1.get_update_budget(market, cur_time)
        targeting_group = api1.get_update_targeting_group(market, cur_time)
        keyword = api1.get_update_keyword(market, cur_time)
        SKU = api1.get_update_sku(market, cur_time)
        # print(budget)
        # print(targeting_group)
        # print(keyword)
        # print(SKU)
        all_table = [[cur_time, "预算优化", budget],
                     [cur_time, "广告位置优化", targeting_group],
                     [cur_time, "关键词优化", keyword],
                     [cur_time, "SKU优化", SKU]
                     ]
        # print(json.dumps(all_table,ensure_ascii=False, default=default_dump))
        get_request_data(market, cur_time, "D-Trim", all_table, 0, brand)
        all_table2 = [[cur_time, "量化广告分析", all_count],
                      [cur_time, "新建广告活动", new_create]
                      ]
        get_request_data(market, cur_time, "D-CALL", all_table2, 0, brand)
        # print(json.dumps(all_table2,ensure_ascii=False, default=default_dump))


def update_data_period(market, brand):
    """将历史操作数据csv上传到线上数据库"""
    today = datetime.today()
    for i in range(30):
        cur_time = (today - timedelta(days=i + 4)).strftime('%Y-%m-%d')
        api1 = AmazonMysqlRagUitl(brand, market)
        campaignName_info, bid_adjust_info = api1.get_data_campaign(market, cur_time)
        if campaignName_info and bid_adjust_info:
            for campaignName, bid_adjust in zip(campaignName_info, bid_adjust_info):
                table2 = [
                    [market, cur_time, "广告活动预算", campaignName, "", "关闭" if bid_adjust == 0 else bid_adjust]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, advertisedSku_info, type_info = api1.get_data_sku(market, cur_time)
        if campaignName_info and advertisedSku_info and type_info:
            for campaignName, advertisedSku, type1 in zip(campaignName_info, advertisedSku_info, type_info):
                if type1 == "手动_关闭":
                    sku_status = "关闭"
                elif type1 == "自动_关闭":
                    sku_status = "关闭"
                elif type1 == "手动_复开":
                    sku_status = "复开"
                elif type1 == "自动_复开":
                    sku_status = "复开"
                elif type1 == "SD_关闭":
                    sku_status = "关闭"
                elif type1 == "SD_复开":
                    sku_status = "复开"
                else:
                    sku_status = "关闭"
                table2 = [[market, cur_time, "SKU状态", campaignName, advertisedSku, sku_status]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, placementClassification_info, bid_adjust_info = api1.get_data_campaign_placement(market,
                                                                                                            cur_time)
        if campaignName_info and placementClassification_info and bid_adjust_info:
            for campaignName, placementClassification, bid_adjust in zip(campaignName_info,
                                                                         placementClassification_info, bid_adjust_info):
                table2 = [[market, cur_time, "广告位竞价", campaignName, placementClassification,
                           "将广告位竞价设置为0" if bid_adjust == 0 else bid_adjust]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, matchType_info, keyword_info, bid_adjust_info = api1.get_data_keyword(market, cur_time)
        if campaignName_info and matchType_info and keyword_info and bid_adjust_info:
            for campaignName, matchType, keyword, bid_adjust in zip(campaignName_info, matchType_info, keyword_info,
                                                                    bid_adjust_info):
                if matchType == "BROAD":
                    keyword_type = "关键词_广泛匹配"
                elif matchType == "PHRASE":
                    keyword_type = "关键词_短语匹配"
                elif matchType == "EXACT":
                    keyword_type = "关键词_精准匹配"
                else:
                    keyword_type = None  # 或者设置为其他默认值，或者不赋值

                if keyword_type is not None:
                    if bid_adjust == -1:
                        bid_adjust_desc = "设置竞价为0.05"
                    else:
                        bid_adjust_desc = "根据ACOS值降价" if bid_adjust == 0 else bid_adjust
                    table2 = [[market, cur_time, keyword_type, campaignName, keyword, bid_adjust_desc]]
                    get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, keyword_info, bid_adjust_info = api1.get_data_automatic_targeting(market, cur_time)
        if campaignName_info and keyword_info and bid_adjust_info:
            for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
                table2 = [[market, cur_time, "自动定位组竞价", campaignName, keyword,
                           "设置竞价为0.05" if bid_adjust == -1 else bid_adjust]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, keyword_info, bid_adjust_info = api1.get_data_product_targets(market, cur_time)
        if campaignName_info and keyword_info and bid_adjust_info:
            for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
                if bid_adjust == -1 or bid_adjust == -10:
                    bid_adjust_desc = "设置竞价为0.05"
                else:
                    bid_adjust_desc = "根据ACOS值降价" if bid_adjust == 0 else bid_adjust
                table2 = [[market, cur_time, "商品投放竞价", campaignName, keyword, bid_adjust_desc]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)


def update_data_manual(market, brand):
    """将每日手动操作数据上传到线上数据库"""
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    cur_time = yesterday.strftime('%Y-%m-%d')
    # cur_time = '2024-08-25'
    campaignName_info, advertisedSku_info, type_info = api(brand, market).get_sku_state_info(market, cur_time)
    if campaignName_info and advertisedSku_info and type_info:
        for campaignName, advertisedSku, type1 in zip(campaignName_info, advertisedSku_info, type_info):
            if type1 == "ENABLED":
                sku_status = "复开"
            elif type1 == "PAUSED":
                sku_status = "关闭"
            else:
                sku_status = "关闭"
            table2 = [[market, cur_time, "SKU状态", campaignName, advertisedSku, sku_status]]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
    campaignName_info, keyword_info, matchType_info, bid_adjust_info = api(brand, market).get_keyword_bid_info(market, cur_time)
    if campaignName_info and matchType_info and keyword_info and bid_adjust_info:
        for campaignName, matchType, keyword, bid_adjust in zip(campaignName_info, matchType_info, keyword_info,
                                                                bid_adjust_info):
            if matchType == "KEYWORD_BROAD":
                keyword_type = "关键词_广泛匹配"
            elif matchType == "KEYWORD_PHRASE":
                keyword_type = "关键词_短语匹配"
            elif matchType == "KEYWORD_EXACT":
                keyword_type = "关键词_精准匹配"
            else:
                keyword_type = None  # 或者设置为其他默认值，或者不赋值

            if keyword_type is not None:
                table2 = [[market, cur_time, keyword_type, campaignName, keyword, bid_adjust]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
    campaignName_info, keyword_info, bid_adjust_info = api(brand, market).get_automatic_targeting_bid_info(market, cur_time)
    if campaignName_info and keyword_info and bid_adjust_info:
        for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
            table2 = [[market, cur_time, "自动定位组竞价", campaignName, keyword,
                       "设置竞价为0.05" if bid_adjust == -1 else bid_adjust]]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
    campaignName_info, keyword_info, bid_adjust_info = api(brand, market).get_product_target_bid_info(market, cur_time)
    if campaignName_info and keyword_info and bid_adjust_info:
        for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
            if bid_adjust == -1 or bid_adjust == -10:
                bid_adjust_desc = "设置竞价为0.05"
            else:
                bid_adjust_desc = "根据ACOS值降价" if bid_adjust == 0 else bid_adjust
            table2 = [[market, cur_time, "商品投放竞价", campaignName, keyword, bid_adjust_desc]]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)


def update_data_manual_period(market, brand):
    """将历史手动操作数据csv上传到线上数据库"""
    start_date = datetime.strptime('2024-07-26', '%Y-%m-%d')
    today = datetime.today()
    yesterday = today - timedelta(days=2)

    # 循环遍历日期范围
    current_date = start_date
    while current_date <= yesterday:
        cur_time = current_date.strftime('%Y-%m-%d')
        api1 = api(brand, market)
        campaignName_info, advertisedSku_info, type_info = api1.get_sku_state_info(market, cur_time)
        if campaignName_info and advertisedSku_info and type_info:
            for campaignName, advertisedSku, type1 in zip(campaignName_info, advertisedSku_info, type_info):
                if type1 == "ENABLED":
                    sku_status = "复开"
                elif type1 == "PAUSED":
                    sku_status = "关闭"
                else:
                    sku_status = "关闭"
                table2 = [[market, cur_time, "SKU状态", campaignName, advertisedSku, sku_status]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, keyword_info, matchType_info, bid_adjust_info = api1.get_keyword_bid_info(market, cur_time)
        if campaignName_info and matchType_info and keyword_info and bid_adjust_info:
            for campaignName, matchType, keyword, bid_adjust in zip(campaignName_info, matchType_info, keyword_info,
                                                                    bid_adjust_info):
                if matchType == "KEYWORD_BROAD":
                    keyword_type = "关键词_广泛匹配"
                elif matchType == "KEYWORD_PHRASE":
                    keyword_type = "关键词_短语匹配"
                elif matchType == "KEYWORD_EXACT":
                    keyword_type = "关键词_精准匹配"
                else:
                    keyword_type = None  # 或者设置为其他默认值，或者不赋值

                if keyword_type is not None:
                    table2 = [[market, cur_time, keyword_type, campaignName, keyword, bid_adjust]]
                    get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, keyword_info, bid_adjust_info = api1.get_automatic_targeting_bid_info(market, cur_time)
        if campaignName_info and keyword_info and bid_adjust_info:
            for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
                table2 = [[market, cur_time, "自动定位组竞价", campaignName, keyword,
                           "设置竞价为0.05" if bid_adjust == -1 else bid_adjust]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, keyword_info, bid_adjust_info = api1.get_product_target_bid_info(market, cur_time)
        if campaignName_info and keyword_info and bid_adjust_info:
            for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
                if bid_adjust == -1 or bid_adjust == -10:
                    bid_adjust_desc = "设置竞价为0.05"
                else:
                    bid_adjust_desc = "根据ACOS值降价" if bid_adjust == 0 else bid_adjust
                table2 = [[market, cur_time, "商品投放竞价", campaignName, keyword, bid_adjust_desc]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        # 准备下一次循环
        current_date += timedelta(days=1)


def update_create_data(market, brand):
    """将每日创建数据上传到线上数据库"""
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    cur_time = yesterday.strftime('%Y-%m-%d')
    # cur_time = '2024-08-25'
    print(cur_time)
    campaignName_info, campaign_type_info, budget_info = AmazonMysqlRagUitl(brand, market).get_create_campaign(market, cur_time)
    if campaignName_info and campaign_type_info and budget_info:
        for campaignName, campaign_type, budget in zip(campaignName_info, campaign_type_info, budget_info):
            table2 = [[market, cur_time, "广告活动创建", campaignName, campaign_type, f'预算为{budget}']]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
    campaignName_info, adGroupName_info, defaultBid_info = AmazonMysqlRagUitl(brand, market).get_create_adgroup(market, cur_time)
    if campaignName_info and adGroupName_info and defaultBid_info:
        for campaignName, adGroupName, defaultBid in zip(campaignName_info, adGroupName_info, defaultBid_info):
            table2 = [[market, cur_time, "广告组创建", campaignName, adGroupName, f'预算为{round(defaultBid, 2)}']]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
    campaignName_info, advertisedSku_info = AmazonMysqlRagUitl(brand, market).get_create_sku(market, cur_time)
    if campaignName_info and advertisedSku_info:
        for campaignName, advertisedSku in zip(campaignName_info, advertisedSku_info):
            table2 = [[market, cur_time, "SKU添加", campaignName, advertisedSku, ""]]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
    campaignName_info, matchType_info, keyword_info, bid_info = AmazonMysqlRagUitl(brand, market).get_create_keyword(market, cur_time)
    if campaignName_info and matchType_info and keyword_info and bid_info:
        for campaignName, matchType, keyword, bid in zip(campaignName_info, matchType_info, keyword_info, bid_info):
            if matchType == "BROAD":
                keyword_type = "关键词创建_广泛匹配"
            elif matchType == "PHRASE":
                keyword_type = "关键词创建_短语匹配"
            elif matchType == "EXACT":
                keyword_type = "关键词创建_精准匹配"
            else:
                keyword_type = None  # 或者设置为其他默认值，或者不赋值
            table2 = [[market, cur_time, keyword_type, campaignName, keyword, f'竞价为{round(bid, 2)}']]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
    campaignName_info, keyword_info, bid_adjust_info = AmazonMysqlRagUitl(brand, market).get_create_product_targets(market, cur_time)
    if campaignName_info and keyword_info and bid_adjust_info:
        for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
            table2 = [[market, cur_time, "商品投放创建", campaignName, keyword, f'竞价为{round(float(bid_adjust), 2)}']]
            get_request_data(market, cur_time, "D-LOG", table2, 0, brand)


def update_create_data_period(market, brand):
    """将历史创建数据上传到线上数据库"""
    today = datetime.today()
    for i in range(30):
        cur_time = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        print(cur_time)
        api1 = AmazonMysqlRagUitl(brand, market)
        campaignName_info, campaign_type_info, budget_info = api1.get_create_campaign(market, cur_time)
        if campaignName_info and campaign_type_info and budget_info:
            for campaignName, campaign_type, budget in zip(campaignName_info, campaign_type_info, budget_info):
                table2 = [[market, cur_time, "广告活动创建", campaignName, campaign_type, f'预算为{budget}']]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, adGroupName_info, defaultBid_info = api1.get_create_adgroup(market, cur_time)
        if campaignName_info and adGroupName_info and defaultBid_info:
            for campaignName, adGroupName, defaultBid in zip(campaignName_info, adGroupName_info, defaultBid_info):
                table2 = [[market, cur_time, "广告组创建", campaignName, adGroupName, f'预算为{round(defaultBid, 2)}']]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, advertisedSku_info = api1.get_create_sku(market, cur_time)
        if campaignName_info and advertisedSku_info:
            for campaignName, advertisedSku in zip(campaignName_info, advertisedSku_info):
                table2 = [[market, cur_time, "SKU添加", campaignName, advertisedSku, ""]]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, matchType_info, keyword_info, bid_info = api1.get_create_keyword(market, cur_time)
        if campaignName_info and matchType_info and keyword_info and bid_info:
            for campaignName, matchType, keyword, bid in zip(campaignName_info, matchType_info, keyword_info, bid_info):
                if matchType == "BROAD":
                    keyword_type = "关键词创建_广泛匹配"
                elif matchType == "PHRASE":
                    keyword_type = "关键词创建_短语匹配"
                elif matchType == "EXACT":
                    keyword_type = "关键词创建_精准匹配"
                else:
                    keyword_type = None  # 或者设置为其他默认值，或者不赋值
                table2 = [[market, cur_time, keyword_type, campaignName, keyword, f'竞价为{round(bid, 2)}']]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)
        campaignName_info, keyword_info, bid_adjust_info = api1.get_create_product_targets(market, cur_time)
        if campaignName_info and keyword_info and bid_adjust_info:
            for campaignName, keyword, bid_adjust in zip(campaignName_info, keyword_info, bid_adjust_info):
                table2 = [
                    [market, cur_time, "商品投放创建", campaignName, keyword, f'竞价为{round(float(bid_adjust), 2)}']]
                get_request_data(market, cur_time, "D-LOG", table2, 0, brand)


# create_summarize_data()
# get_request_data('FR', '2024-08-21', 'B-TABLE', [
#     ['FR', '2024-08-07', '1399.58', '220.03', '15.72%', '220.03', '1399.58', '15.72%', '100.00%', '0.0', '0.0', '0',
#      '0.00%', '2138.61', '1399.58', '739.03', '34.56%'],
#     ['FR', '2024-08-08', '959.01', '192.49', '20.07%', '191.84', '959.01', '20.00%', '100.00%', '0.0', '0.65', '0',
#      '0.00%', '1071.41', '959.01', '112.4', '10.49%']], 0, 'OutdoorMaster')
# time.sleep(60 * 60 * 5)
# brands_and_countries = {U-TABLE
#     'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK", "JP"],
#     'DELOMO': ['IT', 'ES', 'DE', 'FR'],
#     'OutdoorMaster': ['IT', 'ES', 'FR', 'SE'],
#     'MUDEELA': ['US'],
#     'Rossny': ['US'],
#     'ZEN CAVE': ['US']
# }
# # brands_and_countries = {
# #     'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK", "JP"],
# #     'DELOMO': ['IT', 'ES', 'DE', 'FR'],
# #     'OutdoorMaster': ['IT', 'ES', 'FR', 'SE']
# # }
# # brands_and_countries = {
# #     'LAPASA': ["ES"],
# # }
# while True:
#     for brand, countries in brands_and_countries.items():
#         for country in countries:
#             get_data(country, brand)
#     print('done')
#     time.sleep(60 * 60 * 24)
# 4-1 2-12
# table2 = [["DE","2024-08-07","关键词_短语匹配","DeepBI_0502_M35 manu_overstock","débardeur homme","根据ACOS值降价"]]
# get_request_data('DE','2024-08-07',"D-LOG",table2,0,'LAPASA')
