from ai.backend.util.db.auto_process.selling_partner.util.client_config import get_ads_config, get_sp_config
from ai.backend.util.db.auto_process.selling_partner.util.auth import Auth
import time
import os
import json


# region = "NA"
# client_config = get_ads_config(region)
# client_id = client_config['client-id']
# client_secret = client_config['client-secret']
# redirect_uri = "https://albert.asas.com"
# refresh_token = client_config['refresh-token']


def call_remote_token(region="NA", now_timestamp=0, file_path="access_tokens.json", API_TYPE="AD"):
    if API_TYPE == "SP":
        client_config = get_sp_config(region)
    else:
        client_config = get_ads_config(region)
    client_id = client_config['client-id']
    client_secret = client_config['client-secret']
    redirect_uri = "https://albert.asas.com"
    refresh_token = client_config['refresh-token']
    auth_api = Auth(client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=redirect_uri,
                    region=region)
    access_token = auth_api.get_new_access_token(refresh_token=refresh_token)
    # with open('.access_token', 'w') as file:
    #     file.write(access_token)
    # now_timestamp = str(int(time.time()) + 3500)
    # with open('.access_token_overtime', 'w') as file:
    #     file.write(now_timestamp)
    # with open('.access_token_region', 'w') as file:
    #     file.write(region)
    # print("access_token get new")
    # 读取整个文件，更新特定地区的数据
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        data = {}

    data[region] = {
        "access_token": access_token,
        "access_token_overtime": now_timestamp + 3500,
        "access_token_region": region
    }

    # 重写整个文件
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

    print("Access token for region", region, "updated.")
    return access_token


def get_access_token(region="NA", API_TYPE="AD"):
    now_timestamp = int(time.time())
    if API_TYPE == "SP":
        file_path = "setting/access_tokens_sp_api.json"
    else:
        file_path = "setting/access_tokens.json"

    # Ensure the directory exists before trying to open the file
    os.makedirs(os.path.dirname(file_path), exist_ok=True)


    # # 判断文件是否存在 不存在则创建
    # if not os.path.isfile(file_path):
    #     with open(file_path, 'w') as file:
    #         file.write("{}")
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            region_data = data.get(region, {})
    except FileNotFoundError:
        region_data = {}

    if region_data:
        access_token_region = region_data.get("access_token_region")
        over_time = int(region_data.get("access_token_overtime", 0))
        if access_token_region != region or now_timestamp > over_time:
            print("Access token for region", region, "expired or not found. Getting new token.")
            # now_timestamp over_time
            print(now_timestamp, over_time, "now_timestamp over_time")
            access_token = call_remote_token(region, now_timestamp, file_path, API_TYPE)
        else:
            access_token = region_data.get("access_token")
    else:
        access_token = call_remote_token(region, 0, file_path, API_TYPE)
    return access_token
