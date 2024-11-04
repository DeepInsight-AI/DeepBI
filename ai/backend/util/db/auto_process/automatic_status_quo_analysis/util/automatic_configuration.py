import json
import os

import yaml

from ai.backend.util.db.auto_process.summary.util.InserOnlineData import ProcessShowData as psd, ProcessShowData
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.main import find_brand_by_uid
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.sql.create_log import create_log
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.sql.select_brand import select_brand
from ai.backend.util.db.configuration.path import get_config_path



def update_brand_info(db,brand_info, new_info):
    # 读取现有的 YAML 文件
    brand_path = os.path.join(get_config_path(), 'Brand1.yml')
    if os.path.exists(brand_path):
        with open(brand_path, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
    else:
        # 如果文件不存在，则创建一个新的文件
        with open(brand_path, 'w', encoding='utf-8') as file:
            yaml.dump({}, file)  # 创建一个空的 YAML 文件
            data = {}  # 初始化为一个空字典

    if data is None:
        data = {}  # 或者根据需要设置其他默认值

    # 更新品牌信息
    if db not in data:
        data[db] = {}

    for brand in brand_info:
        # 更新 JSON 数据，使用 new_info 的拷贝
        data[db][brand] = {"default": new_info.copy()}

    # 写回到文件
    with open(brand_path, 'w', encoding='utf-8') as file:
        yaml.dump(data, file)
    print(f"Updated Brand1.yml with new dbinfo: {new_info}")


def update_db_info(db,new_dbinfo,brand_info,db_json):
    try:
        # 读取现有的 JSON 文件
        db_info_path = os.path.join(get_config_path(), db_json)
        with open(db_info_path, 'r') as f:
            data = json.load(f)

        # 初始化 db 对应的字典
        data[db] = {}

        for brand in brand_info:
            # 更新 JSON 数据
            data[db][brand] = {"default": new_dbinfo}

        # 写回更新后的 JSON 数据
        with open(db_info_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Updated db_info.json with new dbinfo: {new_dbinfo}")

    except FileNotFoundError:
        print(f"Error: {db_info_path} not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {db_info_path}.")
    except Exception as e:
        print(f"Unexpected error: {e}")

def get_record_by_id(data, target_id):
    for record in data:
        if record.get('ID') == target_id:
            return record
    return None


def automatic_configuration():
    data = {
        "CloseFlag": 0  # 1 关闭的 0 没有关闭的
    }
    data, msg = ProcessShowData.user_account_info(post_data=data)
    print(data, msg)
    for i in range(1, 99):
        print(i)
        print('-------------------------')
        db, brand_name, brand_info = find_brand_by_uid(i)
        if db and brand_name and brand_info:
            continue
        else:

            record = get_record_by_id(msg['data'], i)
            if record is None:
                continue
            else:
                if record['DbName'] and record['LogDbName']:
                    # 分割 DbName 和 LogDbName
                    db_names = record['DbName'].split(',')
                    log_db_names = record['LogDbName'].split(',')

                    # 处理 DbName 和 LogDbName
                    for db_name, log_db_name in zip(db_names, log_db_names):
                        print(f"DbName: {db_name.strip()}, LogDbName: {log_db_name.strip()}")
                        db_info = db_name.strip()
                        brand_info = select_brand(db_info)
                        print(brand_info)
                        new_info = {
                            'host': "192.168.5.114",
                            'user': "root",
                            'password': "duozhuan888",
                            'dbname': db_info,
                            'port': 3308,
                            'UID': i,
                            'public': 1,
                            'api_type': "OLD",
                        }
                        update_brand_info(db_info,brand_info,new_info)
                        db_new_info = {
                            "host": "192.168.2.139",
                            "user": "wanghequan",
                            "passwd": "WHq123123Aa",
                            "port": 3306,
                            "db": db_info,
                            "charset": "utf8mb4",
                            "use_unicode": True
                        }
                        update_db_info(db_info,db_new_info,brand_info,'db_info.json')
                        log_db_info = log_db_name.strip()
                        # create_log(log_db_info)
                        db_log_info = {
                            "host": "192.168.2.123",
                            "user": "wanghequan",
                            "passwd": "WHq123123Aa",
                            "port": 3306,
                            "db": log_db_info,
                            "charset": "utf8mb4",
                            "use_unicode": True
                        }
                        # Update the JSON file with new dbinfo
                        update_db_info(db_info,db_log_info,brand_info,'db_info_log.json')
                        print(f"{i} done")
                        pass



if __name__ == '__main__':
    automatic_configuration()
    # res = find_brand_by_uid(1)
    # print(res)
