from preprocessing_data import preprocess_daily_data,preprocess_sp_data,preprocess_overstock_data,preprocess_daily_data_anomaly_detection
from llm_processing import llm_processing,llm_processing1,processing,llm_processing2,llm_processing3
from auto_execute import auto_execute,auto_execute_rollback,auto_execute1,auto_execute2
from preprocess_csv import preprocess_csv
from datetime import datetime
import time


def run():
    #time.sleep(60 * 60 * 2)
    # 'IT', 'ES', 'DE', 'FR'
    # 'IT', 'ES', 'DE', 'UK', 'US'
    # 'IT' ,'ES', 'DE', 'FR', 'UK', 'US'
    # OutdoorMaster
    # 'IT', 'ES', 'FR'
    # brands_and_countries = {
    #     'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US'],
    #     'DELOMO': ['IT', 'ES', 'DE', 'FR'],
    #     'OutdoorMaster': ['IT', 'ES', 'FR']
    # }
    brands_and_countries = {
        'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US']
    }
    while True:
        for brand, countries in brands_and_countries.items():
            for country in countries:

                today = datetime.today()
                cur_time = today.strftime('%Y-%m-%d')
                #cur_time = '2024-07-11'
                current_country = country

                # 预处理数据，生成相应的csv,初次运行会生成json文件，用来描述各个字段的意思
                preprocess_daily_data(cur_time, current_country, brand)
                processing(cur_time, current_country, use_llm=False, brand=brand, strategy='daily')
                llm_processing(cur_time, current_country, 'default', brand)  # deepseek
                if brand == 'LAPASA':
                    llm_processing3(cur_time, current_country, 'default', brand, version=0)  # deepseek
                    preprocess_sp_data(cur_time, current_country, brand)
                    llm_processing1(cur_time, current_country, 'default', brand)
                preprocess_csv(cur_time, current_country, brand, strategy='daily')
                if brand == 'LAPASA':
                    preprocess_daily_data_anomaly_detection(cur_time, current_country, brand)
                auto_execute(cur_time, current_country,brand=brand, strategy='daily')



        time.sleep(60 * 60 * 24)


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
            #llm_processing2(cur_time, current_country, 'default', brand[0])  # deepseek
            preprocess_csv(cur_time, current_country, brand[0], strategy='overstock')
            auto_execute1(cur_time, current_country, brand=brand[0], strategy='overstock')

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

if __name__ == "__main__":
    run1()

