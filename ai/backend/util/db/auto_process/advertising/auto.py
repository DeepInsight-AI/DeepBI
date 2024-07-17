from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.gen_sp_campaign import Gen_campaign as sp


def change_campaign_Bidding_strategy(market,brand_name):
    api1 = DbSpTools(brand_name)
    campaignId_info = api1.select_sp_campaign(market)
    if campaignId_info:
        for campaignId in campaignId_info:
            api2 = sp(brand_name)
            api2.update_camapign_Bidding_strategy(market, campaignId)
        print(f"update success")
        print('_' * 30)
        print('_' * 30)
        print('_' * 30)
    print("all update success")

change_campaign_Bidding_strategy(market='ES',brand_name='LAPASA')


