import json
import os

from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_yzj.preprocessing_data import preprocess_daily_data,preprocess_daily_data_test,preprocess_sp_data,preprocess_overstock_data,preprocess_daily_data_anomaly_detection
from ai.backend.util.db.auto_yzj.llm_processing import llm_processing,llm_processing1,processing,llm_processing2,llm_processing3,processing_test
from ai.backend.util.db.auto_yzj.auto_execute import auto_execute,auto_execute_rollback,auto_execute1,auto_execute2,auto_execute3
from ai.backend.util.db.auto_yzj.preprocess_csv import preprocess_csv
from datetime import datetime
import time


def run_():
    #time.sleep(60 * 60 * 2)
    # 'IT', 'ES', 'DE', 'FR'
    # 'IT', 'ES', 'DE', 'UK', 'US'
    # 'IT' ,'ES', 'DE', 'FR', 'UK', 'US'
    # OutdoorMaster
    # 'IT', 'ES', 'FR'
    # brands_and_countries = {
    #     'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US', 'JP'],
    #     'DELOMO': ['IT', 'ES', 'DE', 'FR'],
    #     'OutdoorMaster': ['IT', 'ES', 'FR', 'SE'],
    #     'MUDEELA': ['US'],
    #     'Rossny': ['US'],
    #     'ZEN CAVE': ['US']
    # }
    # brands_and_countries = {
    #     'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US', 'JP'],
    #     'DELOMO': ['IT', 'ES', 'DE', 'FR'],
    #     'OutdoorMaster': ['IT', 'ES', 'FR', 'SE', 'JP'],
    #     'MUDEELA': ['US'],
    #     'Rossny': ['US'],
    #     'ZEN CAVE': ['US'],
    #     'Gotoly': ['US'],
    # }
    brands_and_countries = {
        'amazon_ads': {
            'brand': 'LAPASA',
            'countries': ["US", "FR", "IT", "DE", "ES", "UK", "JP"]
        },
        'amazon_bdzx': {
            'brand': 'DELOMO',
            'countries': ['IT', 'ES', 'DE', 'FR']
        },
        'amazon_bdzx_delomo': {
            'brand': 'DELOMO',
            'countries': ['US']
        },
        'amazon_outdoormaster': {
            'brand': 'OutdoorMaster',
            'countries': ['IT', 'ES', 'FR', 'SE']
        },
        'amazon_outdoormaster_jp': {
            'brand': 'OutdoorMaster',
            'countries': ['JP']
        },
        'amazon_bdzx_mudeela': {
            'brand': 'MUDEELA',
            'countries': ['US']
        },
        'amazon_bdzx_rossny': {
            'brand': 'Rossny',
            'countries': ['US']
        },
        'amazon_bdzx_zen_cave': {
            'brand': 'ZEN CAVE',
            'countries': ['US']
        },
        'amazon_chaoyangkeji_gotoly': {
            'brand': 'Gotoly',
            'countries': ['US']
        },
        'amazon_mayigongxiang': {
            'brand': 'ANTSHARE',
            'countries': ['IT', 'ES', 'DE', 'FR']
        },
        'amazon_mayigongxiang_huakey': {
            'brand': 'GOLDJU',
            'countries': ['IT', 'ES', 'DE', 'FR']
        }

    }
    last_main_loop_time = time.time() - 60 * 60 * 24
    while True:
        current_time = time.time()
        if current_time - last_main_loop_time >= 60 * 60 * 24:
            for key, value in brands_and_countries.items():
                brand = value.get('brand', value['brand'])  # Use 'db' if 'brand' not present
                countries = value['countries']
                for country in countries:

                    today = datetime.today()
                    cur_time = today.strftime('%Y-%m-%d')
                    cur_time = '2024-09-24'
                    current_country = country

                    # 预处理数据，生成相应的csv,初次运行会生成json文件，用来描述各个字段的意思
                    preprocess_daily_data_test(cur_time, current_country, brand,key)
                    processing_test(cur_time, current_country, use_llm=False, brand=brand, strategy='daily',db=key)
                    # if brand == 'LAPASA':
                    #     llm_processing(cur_time, current_country, 'default', brand)  # deepseek
                    # 老版本已弃用
                    # if brand == 'LAPASA':
                    #     llm_processing3(cur_time, current_country, 'default', brand, version=0)  # deepseek
                    #     preprocess_sp_data(cur_time, current_country, brand)
                    #     llm_processing1(cur_time, current_country, 'default', brand)
                    preprocess_csv(cur_time, current_country, brand, strategy='daily')
                    auto_execute3(cur_time, current_country, brand=brand, strategy='daily',db=key)
                    #preprocess_daily_data_anomaly_detection(cur_time, current_country, brand)
                    # if brand == 'LAPASA':
                    #     preprocess_daily_data_anomaly_detection(cur_time, current_country, brand)
                    #     # if brand == 'US' or brand == 'UK':
                    #     #     auto_execute1(cur_time, current_country, brand=brand, strategy='daily')
                    #     # else:
                    #     #     auto_execute(cur_time, current_country,brand=brand, strategy='daily')
                    # else:
                    #     auto_execute2(cur_time, current_country, brand=brand, strategy='daily')
                    last_main_loop_time = current_time

        time.sleep(60 * 60 * 1)


def run():
    brands_and_countries = {
        'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US', 'JP'],
        'DELOMO': ['IT', 'ES', 'DE', 'FR', 'US'],
        'OutdoorMaster': ['IT', 'ES', 'FR', 'SE', 'JP'],
        'MUDEELA': ['US'],
        'Rossny': ['US'],
        'ZEN CAVE': ['US'],
        'Gotoly': ['US'],
        'us1': ['US'],
        'us2': ['US'],
        'eu': ['DE', 'UK']
    }

    execution_times = {}  # 用于记录执行时间
    last_main_loop_time = time.time() - 60 * 60 * 24

    # 尝试读取已有的执行时间记录
    execution_path = os.path.join(get_config_path(), 'execution_times.json')
    if os.path.exists(execution_path):
        with open(execution_path, 'r') as json_file:
            execution_times = json.load(json_file)

    while True:
        current_time = time.time()
        if current_time - last_main_loop_time >= 60 * 60 * 24:
            for brand, countries in brands_and_countries.items():
                for country in countries:
                    today = datetime.today()
                    cur_time = today.strftime('%Y-%m-%d')
                    current_country = country

                    # 执行相关处理函数
                    preprocess_daily_data_test(cur_time, current_country, brand)
                    processing_test(cur_time, current_country, use_llm=False, brand=brand, strategy='daily')
                    preprocess_csv(cur_time, current_country, brand, strategy='daily')
                    auto_execute3(cur_time, current_country, brand=brand, strategy='daily')

                    # 更新或记录执行时间
                    if brand not in execution_times:
                        execution_times[brand] = []  # 确保这是一个列表
                    current_time1 = time.time()
                    country_exists = False
                    for entry in execution_times[brand]:
                        if entry['market'] == current_country:
                            entry['timestamp'] = datetime.fromtimestamp(current_time1).strftime('%Y-%m-%d %H:%M:%S')
                            entry['date'] = cur_time
                            country_exists = True
                            break

                    # 如果国家记录不存在，添加新条目
                    if not country_exists:
                        execution_times[brand].append({
                            "market": current_country,
                            "timestamp": datetime.fromtimestamp(current_time1).strftime('%Y-%m-%d %H:%M:%S'),
                            "date": cur_time
                        })

            # 将执行时间记录写入 JSON 文件
            with open(execution_path, 'w') as json_file:
                json.dump(execution_times, json_file, indent=4)

            last_main_loop_time = current_time
            print("Finished")

        time.sleep(60 * 60 * 1)

def run1():
    countrys = ['IT', 'ES', 'DE', 'FR']
    brand = ['LAPASA', 'DELOMO', 'OutdoorMaster']
    index = 0
    while True:
        for index in range(len(countrys)):
            today = datetime.today()
            cur_time = today.strftime('%Y-%m-%d')
            #cur_time = '2024-07-04'
            current_country = countrys[index]

            # 预处理数据，生成相应的csv,初次运行会生成json文件，用来描述各个字段的意思
            preprocess_overstock_data(cur_time, current_country, brand[0])
            processing(cur_time, current_country, use_llm=False, brand=brand[0], strategy='overstock')
            llm_processing2(cur_time, current_country, 'default', brand[0])  # deepseek
            preprocess_csv(cur_time, current_country, brand[0], strategy='overstock')
            #auto_execute1(cur_time, current_country, brand=brand[0], strategy='overstock')

        time.sleep(60 * 60 * 24)


def run2():
    #time.sleep(60 * 60 * 2)
    # DELOMO
    # 'IT', 'ES', 'DE', 'FR'
    # LAPASA
    # 'IT', 'ES', 'DE', 'UK', 'US'
    # 'IT' ,'ES', 'DE', 'FR', 'UK', 'US'
    # OutdoorMaster
    # 'IT', 'ES', 'FR'
    countrys = ['DE']
    brand = ['LAPASA', 'DELOMO', 'OutdoorMaster']
    index = 0
    while True:
        for index in range(len(countrys)):
            today = datetime.today()
            #cur_time = today.strftime('%Y-%m-%d')
            cur_time = '2024-07-02'
            current_country = countrys[index]

            # 预处理数据，生成相应的csv,初次运行会生成json文件，用来描述各个字段的意思
            # preprocess_daily_data(cur_time, current_country, brand[0])
            # processing(cur_time, current_country, use_llm=False, brand=brand[0], strategy='daily')
            # llm_processing(cur_time, current_country, 'default', brand[0])  # deepseek
            # # #llm_processing(cur_time, current_country, llm='default', version=0)  # deepseek
            # # #preprocess_sp_data(cur_time, current_country)
            # # #llm_processing1(cur_time, current_country, llm='default')
            # preprocess_csv(cur_time, current_country, brand[0], strategy='daily')
            auto_execute_rollback(cur_time, current_country,brand=brand[2], strategy='daily')


        time.sleep(60 * 60 * 24)

def _run(db,brand,market,user):
    try:
        today = datetime.today()
        cur_time = today.strftime('%Y-%m-%d')
        # cur_time = '2024-09-24'
        current_country = market

        # 预处理数据，生成相应的csv,初次运行会生成json文件，用来描述各个字段的意思
        preprocess_daily_data_test(cur_time, current_country, brand, db)
        processing_test(cur_time, current_country, use_llm=False, brand=brand, strategy='daily', db=db)
        # if brand == 'LAPASA':
        #     llm_processing(cur_time, current_country, 'default', brand)  # deepseek
        preprocess_csv(cur_time, current_country, brand, strategy='daily')
        auto_execute3(cur_time, current_country, brand=brand, strategy='daily', db=db, user=user)
        return 200
    except Exception as e:
        print(e)
        return 500


if __name__ == "__main__":
    run_()

