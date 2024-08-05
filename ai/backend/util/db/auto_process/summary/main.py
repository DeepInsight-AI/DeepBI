import time
from ai.backend.util.db.auto_process.summary.summary import get_data

def run():
    # time.sleep(60 * 60 * 5)
    brands_and_countries = {
        'LAPASA': ["US", "FR", "IT", "DE", "NL", "SE", "ES", "UK", "JP"],
        'DELOMO': ['IT', 'ES', 'DE', 'FR'],
        'OutdoorMaster': ['IT', 'ES', 'FR', 'SE'],
        'MUDEELA': ['US'],
        'Rossny': ['US'],
        'ZEN CAVE': ['US']
    }
    while True:
        for brand, countries in brands_and_countries.items():
            for country in countries:
                get_data(country, brand)
        print('done')
        time.sleep(60 * 60 * 24)

if __name__ == "__main__":
    run()

# LAPASA SE  JP  D-Trim
# MUDEELA US  D-CALL
# ZEN CAVE US D-Trim
