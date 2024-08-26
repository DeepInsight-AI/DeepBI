from ai.backend.util.db.auto_process.provide_api.method.sp_api import auto_api_sp
from ai.backend.util.db.auto_process.provide_api.method.sd_api import auto_api_sd
from ai.backend.util.db.auto_process.create_new_sp_ad_auto import Ceate_new_sku


def create_api(data):
    if data['type'] == 'SP':
        code = sp_api(data)
    elif data['type'] == 'SD':
        code = sd_api(data)
    return code

def sp_api(data):
    api = Ceate_new_sku()
    if data['replication'] == 'True':
        pass
        # if data['position'] == 'campaign':
        #     code = api.update_sp_ad_budget(data['ID'], data['text'])
        # elif data['position'] == 'placement':
        #     code = api.update_sp_ad_placement(data['ID'], data['text'], data['placement'])
        # elif data['position'] == 'keyword':
        #     code = api.update_sp_ad_keyword(data['ID'], data['text'])
        # elif data['position'] == 'product_target':
        #     code = api.update_sp_ad_product_targets(data['ID'], data['text'])
        # elif data['position'] == 'automatic_targeting':
        #     code = api.update_sp_ad_automatic_targeting(data['ID'], data['text'])
    elif data['replication'] == 'False':
        if data['strategy'] == 'manual':
            try:
                api.create_new_sp_manual_no_template_jiutong_api(data['market'], data['brand'], data['text'], data['budget'])
                return 200
            except Exception as e:
                print(e)
                return 500  # Internal Server Error
        # elif data['position'] == 'sku':
        #     code = api.auto_sku_status(data['ID'], data['text'])
        # elif data['position'] == 'keyword':
        #     code = api.auto_keyword_status(data['ID'], data['text'])
        # elif data['position'] == 'product_target':
        #     code = api.auto_targeting_status(data['ID'], data['text'])
        # elif data['position'] == 'automatic_targeting':
        #     code = api.auto_targeting_status(data['ID'], data['text'])

def sd_api(data):
    pass
    # api = auto_api_sd(data['brand'], data['market'])
    # if data['require'] == 'bid':
    #     if data['position'] == 'campaign':
    #         code = api.update_sd_ad_budget(data['ID'], data['text'])
    #     elif data['position'] == 'product_target':
    #         code = api.update_sd_ad_product_targets(data['ID'], data['text'])
    # elif data['require'] == 'state':
    #     if data['position'] == 'campaign':
    #         code = api.auto_campaign_status(data['ID'], data['text'])
    #     elif data['position'] == 'sku':
    #         code = api.auto_sku_status(data['ID'], data['text'])
    #     elif data['position'] == 'product_target':
    #         code = api.auto_targeting_status(data['ID'], data['text'])
    # return code
