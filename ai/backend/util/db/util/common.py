import json
import time
import traceback
import pytz
import os
import yaml
from datetime import datetime, timedelta
from ai.backend.util.db.util.db_tool.ad_tool import DbSpTools
from ai.backend.util.db.util.InserOnlineData import ProcessShowData
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.selling_partner.util.client_config import sp_config


def get_profile_id_info(market, brand):
    try:
        profileId,region = DbSpTools(brand,market).get_profileId(market)
        print(profileId)
        return profileId,region
    except Exception as e:
        print(f"Error retrieving profile IDs from database: {e}")
        return None,None


def load_credentials():
    credentials_path = os.path.join(get_config_path(), 'credentials.json')
    #credentials_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_process/credentials.json'
    with open(credentials_path) as f:
        config = json.load(f)
    return config['credentials']


def select_market(market,brand):
    market_credentials = load_credentials().get(market)
    if not market_credentials:
        raise ValueError(f"Market '{market}' not found in credentials")

    brand_credentials = market_credentials.get(brand)
    if not brand_credentials:
        raise ValueError(f"Brand '{brand}' not found in credentials for market '{market}'")

    # 返回相应的凭据和市场信息
    return brand_credentials


def select_brand(brand, country=None):
    # 从 JSON 文件加载数据库信息
    Brand_path = os.path.join(get_config_path(), 'Brand.yml')
    with open(Brand_path, 'r') as file:
        Brand_data = yaml.safe_load(file)

    brand_info = Brand_data.get(brand, {})
    if country and country in brand_info:
        return brand_info[country]
    return brand_info.get('default', {})


def new_get_api_config(brand_config, region, api_type, is_new=False):
    try:
        config_path = os.path.join(get_config_path(), 'config.yml')
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
        return {}

    try:
        if config:
            if config[api_type]:
                name = brand_config['dbname'].replace("amazon_", "")

                # 判断有没有 config/name/api_type 这个文件夹
                brand_file_path = os.path.join(f"config/{name}", region)
                if not os.path.exists(brand_file_path):
                    os.makedirs(brand_file_path)

                # 判断brand_file_path下有没有 api_type.yml 文件
                api_file_path = os.path.join(brand_file_path, f"{api_type}.yml")
                if not os.path.exists(api_file_path):
                    with open(api_file_path, 'w'):  # 创建空的 YAML 文件
                        pass

                # 读取 api_file_path 文件
                with open(api_file_path, "r") as f:
                    api_config = yaml.safe_load(f)
                # 判断api_config 有没有access_token
                print(api_config, "api_config++++++++++++++++++")
                now_timestamp = int(time.time())
                if api_config and 'access_token' in api_config and not is_new and now_timestamp < api_config['expires_timestamp'] - 50:
                    print("本地存在UID token 数据")
                    result = {
                        "client_id": config[api_type]['client_id'],
                        "client_secret": config[api_type]['client_secret'],
                        "refresh_token": api_config['refresh_token'],
                        "access_token": api_config['access_token']
                    }
                else:
                    api_config = {}
                    print("不存在UID token 数据")
                    data = {
                        "UID": brand_config['UID'],  # 所属用户
                        "AreaCode": region,  # 那个地区
                        "OuthType": api_type  # 操作类型
                    }
                    # print("data==", data)
                    res, data = ProcessShowData.get_accesstoken(data)
                    # print("res==", res)
                    # print("data==", data)
                    if res:
                        result = {
                            "client_id": config[api_type]['client_id'],
                            "client_secret": config[api_type]['client_secret'],
                            "refresh_token": data['data']['refresh_token'],
                            "access_token": data['data']['access_token']
                        }
                        # name = brand_config['dbname'].replace("amazon_", "")
                        # if name not in config[api_type]:
                        #     config[api_type][name] = {}
                        # 更新config.yml中指定类型的refresh_token和access_token
                        api_config['UID'] = brand_config['UID']
                        api_config['refresh_token'] = data['data']['refresh_token']
                        api_config['access_token'] = data['data']['access_token']
                        api_config['expires_timestamp'] = data['data']['expires_timestamp']
                        with open(api_file_path, "w") as f:
                            yaml.dump(api_config, f)
        else:
            result = {}
        return result
    except Exception as e:
        print(f"Error retrieving client IDs from database: {e}")
        return {}


def get_sp_my_credentials(market, brand):
    brand_config = select_brand(brand)
    # 判断public在不在brand_config中 且为1
    if 'public' in brand_config and brand_config['public'] == 1:
        print("new==")
        api_config = new_get_api_config(brand_config, region, "SP", logger)
    else:
        print("old==")
        api_config = get_api_config(region, "SP", mysql_con, logger)
    if not api_config:
        print(f"No API config found for region: {region}")
        return {}
    my_credentials = {
        "refresh_token": api_config["refresh_token"],
        "lwa_app_id": api_config["client_id"],
        "lwa_client_secret": api_config["client_secret"],
    }
    return my_credentials


# 获取 ad my_credentials
def get_ad_my_credentials(market, brand):
    brand_config = select_brand(brand,market)
    if 'public' in brand_config and brand_config['public'] == 1:
        profileid,region = get_profile_id_info(market, brand)
        api_config = new_get_api_config(brand_config, region, "AD")
        if not api_config:
            print(f"No API config found for region: {market}")
            return {}
        my_credentials = dict(
            refresh_token=api_config['refresh_token'],
            client_id=api_config['client_id'],
            client_secret=api_config['client_secret'],
            profile_id=str(profileid),
        )
        #print("api_config==", api_config)
        return my_credentials,api_config['access_token']
    else:
        print("old==")
        my_credentials = select_market(market,brand)
        return my_credentials,None


# 根据传递进入的region 判断 只有FE才返回

def get_proxies(region):
    proxies = {
        "http": "http://192.168.5.165:7890",
        "https": "http://192.168.5.165:7890"
    }
    if region == "JP":
        return proxies
    else:
        return {}
