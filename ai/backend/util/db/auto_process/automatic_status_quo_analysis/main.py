import json
import os
import time
from datetime import datetime, timedelta

import yaml

from ai.backend.util.db.auto_process.automatic_status_quo_analysis.util.public_api import ProcessShowData
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.generate_docx_current_situation_analysis import generate_docx,generate_docx_test
from ai.backend.util.db.auto_process.summary.util.InserOnlineData import ProcessShowData as psd
import time

def find_brand_by_uid(uid):
    Brand_path = os.path.join(get_config_path(), 'Brand1.yml')
    if os.path.exists(Brand_path):
        with open(Brand_path, 'r', encoding='utf-8') as file:
            brands = yaml.safe_load(file)
    else:
        # 如果文件不存在，则创建一个新的文件
        with open(Brand_path, 'w', encoding='utf-8') as file:
            yaml.dump({}, file)  # 创建一个空的 YAML 文件
            brands = {}  # 初始化为一个空字典
    if brands is None:
        brands = {}  # 或者根据需要设置其他默认值

    if brands:
        for brand_group, brand_data in brands.items():
            for brand_name, country_data in brand_data.items():
                for country, config in country_data.items():
                    # print(f"Checking brand: {brand_name} with UID: {config.get('UID')}")  # 调试输出
                    if config.get('UID') == uid:
                        return brand_group, brand_name, config
    return None, None, None


def update_db_info(new_dbinfo):
    try:
        # 读取现有的 JSON 文件
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
            data = json.load(f)

        # 更新 JSON 数据
        key = new_dbinfo.get("db")  # 假设你的 dbname 能作为键
        if key:
            data[key] = {key: {"default": new_dbinfo}}
        else:
            print("Error: 'db' key is missing from new_dbinfo")
            return

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


def run():
    # Initialize timing
    last_summary_time = time.time() - 60 * 60 * 2

    while True:
        current_time = time.time()
        # Check if it's time to call create_summarize_data
        if current_time - last_summary_time >= 60 * 60 * 2:
            try:
                users = ProcessShowData.get_all_user()
                print(users)
                success1, data = users
                if success1:
                    # 提取所有的id
                    results = []
                    for item in data['data']:
                        if isinstance(item, dict):
                            # Check type of `item['phone']`
                            phone = item.get('phone')
                            if isinstance(phone, str):
                                phone_last_four = phone[-4:]  # 获取 phone 的最后四位
                                results.append({'id': item['id'], 'phone_last_four': phone_last_four})
                            else:
                                print(f"Unexpected type for phone: {type(phone)}. Data: {item}")
                        else:
                            print(f"Unexpected type for item: {type(item)}. Data: {item}")

                    # 打印结果
                    for result in results:
                        print(f"ID: {result['id']}, Phone Last Four: {result['phone_last_four']}")
                        query_data = {
                            "uid": int(result['id']),
                            "shopid": "",  # 可以不传递
                            "country_code": ""  # 可以不传递
                        }
                        success2, report_info = ProcessShowData.get_report_info(query_data)
                        print(success2, report_info)
                        if success2:
                            # 确保返回的数据的格式和内容是我们期望的
                            if report_info['code'] == 200:
                                for report in report_info['data']:
                                    if report['report_state'] == 1:
                                        shopid = report['shopid']
                                        country_code = report['country_code']
                                        print(f"Shop ID: {shopid}, Country Code: {country_code}")
                                        dbname = "test_amazon_" + result['phone_last_four'] + "_" + str(result['id']) + "_" + str(shopid)
                                        dbinfo = {
                                            "host": "192.168.2.123",
                                            "user": "wanghequan",
                                            "passwd": "WHq123123Aa",
                                            "port": 3306,
                                            "db": dbname,
                                            "charset": "utf8mb4",
                                            "use_unicode": True
                                        }
                                        # Update the JSON file with new dbinfo
                                        update_db_info(dbinfo)

                                        # 获取今天的日期
                                        today = datetime.now()
                                        # 计算今天的前2天
                                        two_days_ago = today - timedelta(days=2)
                                        # 计算今天的前31天
                                        thirty_one_days_ago = today - timedelta(days=31)

                                        # 按照 '%Y-%m-%d' 格式生成日期字符串
                                        two_days_ago_str = two_days_ago.strftime('%Y-%m-%d')
                                        thirty_one_days_ago_str = thirty_one_days_ago.strftime('%Y-%m-%d')

                                        # 打印结果
                                        print("Two days ago:", two_days_ago_str)
                                        print("Thirty one days ago:", thirty_one_days_ago_str)

                                        pdf_path = generate_docx(dbname,dbname,country_code,thirty_one_days_ago_str, two_days_ago_str)
                                        print(pdf_path)
                                        file = pdf_path
                                        data = {
                                            "uid": int(result['id']),
                                            "shopid": str(shopid),
                                            "country_code": country_code,
                                            "send_email": 0  # 是否通知发送邮件 默认0 不发，1 发送，可以不传递，默认不发送(上传与发送分开)
                                        }
                                        result, msg = ProcessShowData.post_file(file, data)
                                        print(result, msg)
                            else:
                                print(f"Error in report_info: {report_info['msg']}")
                        else:
                            print("Request failed")
                else:
                    print("Request failed")
            except Exception as e:
                print(f"An error occurred: {e}")
        if current_time - last_summary_time >= 60 * 60 * 2:
            try:
                for i in range(1, 51):
                    print(i)
                    data = {
                        "UID": i
                    }
                    result, msg = psd.get_user_outh(data)
                    print(result, msg)
                    if result:
                        # Check 'SP' section
                        if 'SP' in msg['data']:
                            for area, sp_data in msg['data']['SP'].items():
                                if 'Outhed_country_code' in sp_data:
                                    for country_entry in sp_data['Outhed_country_code']:
                                        if country_entry['report_state'] == 1:
                                            CountryCode = country_entry['CountryCode']
                                            print(CountryCode)
                                            db, brand_name, brand_info = find_brand_by_uid(i)
                                            print(db, brand_name, brand_info)
                                            if brand_name:
                                                # # Update the JSON file with new dbinfo
                                                # update_db_info(dbinfo)

                                                # 获取今天的日期
                                                today = datetime.now()
                                                # 计算今天的前2天
                                                two_days_ago = today - timedelta(days=2)
                                                # 计算今天的前31天
                                                thirty_one_days_ago = today - timedelta(days=31)

                                                # 按照 '%Y-%m-%d' 格式生成日期字符串
                                                two_days_ago_str = two_days_ago.strftime('%Y-%m-%d')
                                                thirty_one_days_ago_str = thirty_one_days_ago.strftime('%Y-%m-%d')
                                                print(two_days_ago_str,thirty_one_days_ago_str)
                                                pdf_path = generate_docx(db, brand_name, CountryCode, thirty_one_days_ago_str, two_days_ago_str)
                                                file = pdf_path
                                                data = {
                                                    "UID": i,
                                                    "CountryCode": CountryCode,
                                                    "send_email": 0  # 是否发送邮件 1:是  0:否，默认否
                                                }
                                                result, msg = psd.post_file(file, data)
                                                print(result, msg)

            except Exception as e:
                print(f"An error occurred: {e}")
            # Optionally: log the error to a file or a logging service
        time.sleep(60 * 30)  # Sleep for a bit before retrying
# #True {'code': 200, 'data': 5, 'msg': 'success'}
# Shop ID: 1, Country Code: MX
if __name__ == "__main__":
    # run()
    # users = ProcessShowData.get_all_user()
    # print(users)
    file = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_process/automatic_status_quo_analysis/output/COFaR_US_2024-09-11-2024-10-10(30天)_现状分析.pdf"
    data = {
        "UID": "38",
        "CountryCode": "US",
        "send_email": 0  # 是否发送邮件 1:是  0:否，默认否
    }
    result, msg = psd.post_file(file, data)
    print(result, msg)

    data = {
        "UID": 38
    }
    result, msg = psd.get_user_outh(data)
    print(result, msg)

