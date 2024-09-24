import json
import os
import time
from datetime import datetime, timedelta
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.summary.db_tool.ads_db import AmazonMysqlRagUitl as api
from ai.backend.util.db.auto_process.summary.summary import get_data,update_create_data_period,update_create_data,get_data_temporary,update_data_manual_period,update_data_manual,get_data_temporary_period
from ai.backend.util.db.auto_process.summary.summary import create_summarize_data


def run():
    brands_and_countries = {
        'amazon_ads': {
            'brand': 'LAPASA',
            'countries': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK", "JP"]
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
            'countries': ['IT', 'ES', 'FR', 'SE', 'JP']
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
        'amazon_huangjunxi': {
            'brand': 'keimi',
            'countries': ['US']
        },
        'amazon_youniverse_ins_us': {
            'brand': 'us1',
            'countries': ['US']
        },
        'amazon_youniverse_fezibo_us': {
            'brand': 'us2',
            'countries': ['US']
        },
        'amazon_youniverse_eu': {
            'brand': 'eu',
            'countries': ['DE', 'UK']
        }
    }

    # Initialize timing
    last_summary_time = time.time() - 60 * 60 * 24
    last_main_loop_time = time.time() - 60 * 60 * 24

    while True:
        current_time = time.time()

        # Check if it's time to run the main loop tasks
        if current_time - last_main_loop_time >= 60 * 60 * 24:
            for key, value in brands_and_countries.items():
                brand = value.get('brand', value['brand'])  # Use 'db' if 'brand' not present
                countries = value['countries']
                for country in countries:
                    print(country, brand, key)
                    get_data_temporary(country, brand, key)
                    update_create_data(country, brand, key)
                    update_data_manual(country, brand, key)

            # Update the last main loop run time
            last_main_loop_time = current_time
            print('Main tasks done')

        # Check if it's time to call create_summarize_data
        if current_time - last_summary_time >= 60 * 60 * 2:
            create_summarize_data()
            last_summary_time = current_time

        # Sleep for a short period before checking again
        time.sleep(60 * 10)
# def run():
#     # time.sleep(60 * 60 * 5)
#     brands_and_countries = {
#         'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK","JP"],
#         'DELOMO': ['IT', 'ES', 'DE', 'FR', 'US'],
#         'OutdoorMaster': ['IT', 'ES', 'FR', 'SE'],
#         'MUDEELA': ['US'],
#         'Rossny': ['US'],
#         'ZEN CAVE': ['US'],
#         'Veement': ['UK'],
#         'KAPEYDESI': ['SA'],
#         'Gvyugke': ['DE', 'FR', 'IT', 'AU'],
#         'Uuoeebb': ['US'],
#         'Gonbouyoku': ['JP'],
#         'syndesmos': ['DE', 'IT'],
#         'suihuooo': ['US'],
#         'Gotoly': ['US'],
#         'keimi': ['US'],
#         'us1': ['US'],
#         'us2': ['US'],
#         'eu': ['DE', 'UK']
#     }
#     # brands_and_countries = {
#     #     'KAPEYDESI': ['SA'],
#     # }
#     while True:
#         for brand, countries in brands_and_countries.items():
#             for country in countries:
#                 get_data_temporary(country, brand)
#                 update_create_data(country, brand)
#                 update_data_manual(country, brand)
#         print('done')
#         create_summarize_data()
#         time.sleep(60 * 60 * 24)

def run1():
    # time.sleep(60 * 60 * 5)
    brands_and_countries = {
        # 'DELOMO': ['US'],
        # 'Gotoly': ['US'],
        'keimi': ['US']
    }
    while True:
        for brand, countries in brands_and_countries.items():
            for country in countries:
                start_time = '2024-07-31'
                get_data_temporary_period(country, brand, start_time)
                update_data_manual_period(country, brand, start_time)
                update_create_data_period(country, brand, start_time)
        print('done')
        time.sleep(60 * 60 * 24)


if __name__ == "__main__":
    #time.sleep(60 * 60 * 7)
    run()


# LAPASA SE  JP  D-Trim
# MUDEELA US  D-CALL
# ZEN CAVE US D-Trim

