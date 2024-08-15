from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.gen_sp_keyword import Gen_keyword


def clean_keyword(market,brand_name):
    keyword_info_sp = DbSpTools(brand_name,market).select_sp_delete_keyword(market)
    if keyword_info_sp:
        for keywordId in keyword_info_sp:
            Gen_keyword(brand_name).delete_keyword_toadGroup(market,keywordId)

brands_and_countries = {
    'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US', 'JP']
}
for brand, countries in brands_and_countries.items():
    for country in countries:
        clean_keyword(country,brand)
