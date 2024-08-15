import json
import pprint

from ai.backend.util.db.auto_process.create_new_sp_ad_auto import Ceate_new_sku
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools

def auto_process():
    api = Ceate_new_sku()
    #api.create_new_sp_auto_no_template1('US',['L59', 'M121', 'M23', 'M118', 'M108', 'G11', 'L96', 'M103', 'L103', 'M82', 'M131'],'LAPASA')
    api.create_new_sp_auto_no_template_jiutong('FR',['B0CN3FH3CD'],'KAPEYDESI',None)
    #api.create_new_sp_manual_no_template('US',['M100','M118'],'LAPASA')


def extract_na_id(data, result=None):
    """递归遍历 JSON 数据，提取所有的 na 和 id。"""
    if result is None:
        result = []

    if isinstance(data, dict):
        if 'na' in data and 'id' in data:
            result.append((data['na'], data['id']))

        for key, value in data.items():
            if isinstance(value, (dict, list)):
                extract_na_id(value, result)

    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                extract_na_id(item, result)

    return result


def auto_process2(market):
    res = AdGroupTools('LAPASA').list_category(market)

    res_dict = json.loads(res)
    na_id_list = extract_na_id(res_dict)
    for na, id_ in na_id_list:
        DbNewSpTools('LAPASA', market).create_category_info(market, na, id_)
        print(f'na: {na}, id: {id_}')


# 国家代码列表
# countries = ["DE", "NL", "SE", "ES", "UK", "JP"]
#
# # 遍历每个国家代码并调用 auto_process2
# for country in countries:
#     auto_process2(country)

