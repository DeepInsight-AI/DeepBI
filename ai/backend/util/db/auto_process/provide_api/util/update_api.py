from ai.backend.util.db.auto_process.provide_api.method.sp_api import auto_api_sp
from ai.backend.util.db.auto_process.provide_api.method.sd_api import auto_api_sd


def update_api(data):
    if data['type'] == 'SP':
        code = sp_api(data)
    elif data['type'] == 'SD':
        code = sd_api(data)
    return code

def sp_api(data):
    api = auto_api_sp(data['brand'],data['market'],data['db'],data['user'])
    if data['require'] == 'bid':
        if data['position'] == 'campaign':
            code = api.update_sp_ad_budget(data['ID'], data['text'])
        elif data['position'] == 'placement':
            code = api.update_sp_ad_placement(data['ID'], data['text'], data['placement'])
        elif data['position'] == 'keyword':
            code = api.update_sp_ad_keyword(data['ID'], data['text'])
        elif data['position'] == 'product_target':
            code = api.update_sp_ad_product_targets(data['ID'], data['text'])
        elif data['position'] == 'automatic_targeting':
            code = api.update_sp_ad_automatic_targeting(data['ID'], data['text'])
    elif data['require'] == 'bid_batch':
        if data['position'] == 'keyword':
            code = api.update_sp_ad_keyword_batch(data['ID'], data['text'])
        elif data['position'] == 'product_target':
            code = api.update_sp_ad_automatic_targeting_batch(data['ID'], data['text'])
        elif data['position'] == 'automatic_targeting':
            code = api.update_sp_ad_automatic_targeting_batch(data['ID'], data['text'])
    elif data['require'] == 'state':
        if data['position'] == 'campaign':
            code = api.auto_campaign_status(data['ID'], data['text'])
        elif data['position'] == 'sku':
            code = api.auto_sku_status(data['ID'], data['text'])
        elif data['position'] == 'keyword':
            code = api.auto_keyword_status(data['ID'], data['text'])
        elif data['position'] == 'product_target':
            code = api.auto_targeting_status(data['ID'], data['text'])
        elif data['position'] == 'automatic_targeting':
            code = api.auto_targeting_status(data['ID'], data['text'])
        elif data['position'] == 'negative_target':
            code = api.negative_target_status(data['ID'], data['text'])
        elif data['position'] == 'negative_keyword':
            code = api.negative_keyword_status(data['ID'], data['text'])
    elif data['require'] == 'state_batch':
        if data['position'] == 'keyword':
            code = api.auto_keyword_status_batch(data['ID'], data['text'])
        elif data['position'] == 'product_target':
            code = api.auto_targeting_status_batch(data['ID'], data['text'])
    elif data['require'] == 'create':
        if data['position'] == 'product_target':
            code = api.create_product_target(data['ID'], data['text'], data['campaignId'], data['adGroupId'])
        elif data['position'] == 'product_target_asin':
            code = api.create_product_target_asin(data['ID'], data['text'], data['campaignId'], data['adGroupId'])
        elif data['position'] == 'product_target_asin_expended':
            code = api.create_product_target_asin_expended(data['ID'], data['text'], data['campaignId'], data['adGroupId'])
        elif data['position'] == 'keyword':
            code = api.create_keyword(data['ID'], data['text'], data['campaignId'], data['adGroupId'], data['matchType'])
        elif data['position'] == 'negative_target':
            code = api.create_negative_target(data['ID'], data['campaignId'], data['adGroupId'], data['matchType'])
    elif data['require'] == 'create_batch':
        if data['position'] == 'negative_target':
            code = api.create_negative_target_batch(data['ID'], data['campaignId'], data['adGroupId'], data['matchType'])
    elif data['require'] == 'name':
        if data['position'] == 'campaign':
            code = api.auto_campaign_name(data['ID'], data['text'])
    elif data['require'] == 'delete':
        if data['position'] == 'negative_target':
            code = api.delete_negative_target(data['ID'])
        elif data['position'] == 'negative_keyword':
            code = api.delete_negative_keyword(data['ID'])
    return code

def sd_api(data):
    api = auto_api_sd(data['brand'],data['market'],data['db'],data['user'])
    if data['require'] == 'bid':
        if data['position'] == 'campaign':
            code = api.update_sd_ad_budget(data['ID'], data['text'])
        elif data['position'] == 'product_target':
            code = api.update_sd_ad_product_targets(data['ID'], data['text'])
    elif data['require'] == 'state':
        if data['position'] == 'campaign':
            code = api.auto_campaign_status(data['ID'], data['text'])
        elif data['position'] == 'sku':
            code = api.auto_sku_status(data['ID'], data['text'])
        elif data['position'] == 'product_target':
            code = api.auto_targeting_status(data['ID'], data['text'])
    elif data['require'] == 'create':
        if data['position'] == 'product_target':
            code = api.create_product_target(data['ID'], data['text'], data['campaignId'], data['adGroupId'])
        elif data['position'] == 'product_target_new':
            code = api.create_product_target_new(data['ID'], data['text'], data['campaignId'], data['adGroupId'])
        elif data['position'] == 'product_target_asin':
            code = api.create_product_target_asin(data['ID'], data['text'], data['adGroupId'])
    elif data['require'] == 'name':
        if data['position'] == 'campaign':
            code = api.auto_campaign_name(data['ID'], data['text'])
    return code
