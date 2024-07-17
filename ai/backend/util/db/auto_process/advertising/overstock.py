from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.gen_sp_campaign import Gen_campaign as sp
from ai.backend.util.db.auto_process.gen_sd_campaign import Gen_campaign as sd


def overstock_change_name(market,info,brand_name):
    for sspu in info:
        api1 = DbSpTools(brand_name)
        name_info_sp,id_info_sp = api1.select_sp_sspu_name(market, sspu)
        if name_info_sp and id_info_sp:
            for campaignName, campaignId in zip(name_info_sp, id_info_sp):
                name = f"{campaignName}_overstock"
                api2 = sp(brand_name)
                api2.update_camapign_name(market, campaignId, campaignName, name)
        name_info_sd, id_info_sd = api1.select_sd_sspu_name(market, sspu)
        if name_info_sd and id_info_sd:
            for campaignName, campaignId in zip(name_info_sd, id_info_sd):
                name = f"{campaignName}_overstock"
                api3 = sd(brand_name)
                api3.update_camapign_name(market, campaignId, campaignName, name)
        print(f"{sspu} update success")
        print('_' * 30)
        print('_' * 30)
        print('_' * 30)
    print("all update success")

def overstock_rechange_name(market,info,brand_name):
    for sspu in info:
        api1 = DbSpTools(brand_name)
        name_info_sp,id_info_sp = api1.select_sp_sspu_name_overstock(market, sspu)
        if name_info_sp and id_info_sp:
            for campaignName, campaignId in zip(name_info_sp, id_info_sp):
                print(campaignName)
                print(type(campaignName))
                name = campaignName[0:-10]
                api2 = sp(brand_name)
                api2.update_camapign_name(market, campaignId, campaignName, name)
        name_info_sd, id_info_sd = api1.select_sd_sspu_name_overstock(market, sspu)
        if name_info_sd and id_info_sd:
            for campaignName, campaignId in zip(name_info_sd, id_info_sd):
                name = campaignName[0:-10]
                api3 = sd(brand_name)
                api3.update_camapign_name(market, campaignId, campaignName, name)
        print(f"{sspu} update success")
        print('_' * 30)
        print('_' * 30)
        print('_' * 30)
    print("all update success")

# markets = ['DE','IT','ES']
overstock_change_name('UK',['L52', 'M08', 'G19', 'L02', 'M131', 'L55', 'M19', 'M07', 'M06', 'M36', 'M92', 'M05', 'K01', 'M93', 'M35', 'L09', 'L100', 'L82', 'M34', 'M134'],'LAPASA')
# for market in markets:
#
#     overstock_rechange_name(market,['L01', 'M38', 'L59', 'L54', 'L98', 'L103'],'LAPASA')

