import json
import os
import time
from datetime import datetime, timedelta
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.summary.db_tool.ads_db import AmazonMysqlRagUitl as api
from ai.backend.util.db.auto_process.summary.summary import get_data,update_create_data_period,update_create_data,get_data_temporary,update_data_manual_period,update_data_manual
from ai.backend.util.db.auto_process.summary.summary import create_summarize_data


def run():
    # time.sleep(60 * 60 * 5)
    brands_and_countries = {
        'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK","JP"],
        'DELOMO': ['IT', 'ES', 'DE', 'FR'],
        'OutdoorMaster': ['IT', 'ES', 'FR', 'SE'],
        'MUDEELA': ['US'],
        'Rossny': ['US'],
        'ZEN CAVE': ['US'],
        'Veement': ['UK'],
        'KAPEYDESI': ['SA'],
        'Gvyugke': ['DE', 'FR', 'IT', 'AU'],
        'Uuoeebb': ['US'],
        'Gonbouyoku': ['JP'],
        'syndesmos': ['DE', 'IT'],
        'suihuooo': ['US']
    }
    # brands_and_countries = {
    #     'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK"]
    # }
    while True:
        for brand, countries in brands_and_countries.items():
            for country in countries:
                get_data_temporary(country, brand)
                update_create_data(country, brand)
                update_data_manual(country, brand)
        print('done')
        time.sleep(60 * 60 * 24)

def run1():
    # time.sleep(60 * 60 * 5)
    brands_and_countries = {
        'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK", "JP"],
        'DELOMO': ['IT', 'ES', 'DE', 'FR'],
        'OutdoorMaster': ['IT', 'ES', 'FR', 'SE'],
        'MUDEELA': ['US'],
        'Rossny': ['US'],
        'ZEN CAVE': ['US'],
        'Veement': ['UK'],
        'KAPEYDESI': ['SA'],
        'Gvyugke': ['DE', 'FR', 'IT', 'AU'],
        'Uuoeebb': ['US'],
        'Gonbouyoku': ['JP'],
        'syndesmos': ['DE', 'IT'],
        'suihuooo': ['US']
    }
    while True:
        for brand, countries in brands_and_countries.items():
            for country in countries:
                update_data_manual_period(country, brand)
        print('done')
        time.sleep(60 * 60 * 24)


if __name__ == "__main__":
    # run()
    create_summarize_data()

# LAPASA SE  JP  D-Trim
# MUDEELA US  D-CALL
# ZEN CAVE US D-Trim

