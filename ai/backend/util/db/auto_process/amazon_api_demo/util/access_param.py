from ai.backend.util.db.auto_process.amazon_api_demo.util.data_aggregation_table import (data_aggregation_table_spmanual,data_aggregation_table_spasin,
                                                                                         data_aggregation_table_spauto,order_query_method,click_query_method,
                                                                                         impression_query_method,query_query_method,data_aggregation_table_spmanual_targeting_group,
                                                                                         data_aggregation_table_spasin_targeting_group,data_aggregation_table_spauto_targeting_group,
                                                                                         data_aggregation_table_keyword,data_aggregation_table_product_targets,
                                                                                         data_aggregation_table_automatic_targeting,data_aggregation_table_sd,
                                                                                         data_aggregation_table_sd_product_targets,data_aggregation_table_sd_sku,
                                                                                         data_aggregation_table_spmanual_sku,data_aggregation_table_spasin_sku,
                                                                                         data_aggregation_table_spauto_sku,data_aggregation_table_spmanual_search_term,
                                                                                         data_aggregation_table_spasin_search_term,data_aggregation_table_spauto_search_term)
from ai.backend.util.db.auto_process.amazon_api_demo.db_tool.db_tool import AmazonMysqlRagUitl

def access_param(params,brand):
    if params['ad_type'] == 'SP-手动':
        query = data_aggregation_table_spmanual(params)
    elif params['ad_type'] == 'SP-ASIN':
        query = data_aggregation_table_spasin(params)
    elif params['ad_type'] == 'SP-自动':
        query = data_aggregation_table_spauto(params)
    elif params['ad_type'] == 'SD':
        query = data_aggregation_table_sd(params)
    else:
        return None
    #print(params['period'])
    order_query = order_query_method(params, params['period'])
    click_query = click_query_method(params, params['period'])
    impression_query = impression_query_method(params, params['period'])
    query_query = query_query_method(params)
    final_query = query + (order_query or "") + (click_query or "") + (impression_query or "") + (query_query or "")
    print(final_query)
    api1 = AmazonMysqlRagUitl(brand,params['country'])
    if params['price_adjustment_operation'] == '增加':
        bid_adjust = float(params['price_adjustment'])
    elif params['price_adjustment_operation'] == '减少':
        bid_adjust = -float(params['price_adjustment'])
    else:
        bid_adjust = None
    csv_filename = api1.get_select_campaign(final_query, params['cur_time'], bid_adjust)
    return csv_filename

def access_param_self(params,brand):
    if params['ad_type'] == 'SP-手动':
        if params['ad_options'] == '广告位':
            query = data_aggregation_table_spmanual_targeting_group(params)
        elif params['ad_options'] == '预算':
            query = data_aggregation_table_spmanual(params)
        elif params['ad_options'] == '关键词':
            query = data_aggregation_table_keyword(params)
        elif params['ad_options'] == 'SKU':
            query = data_aggregation_table_spmanual_sku(params)
        elif params['ad_options'] == '搜索词':
            query = data_aggregation_table_spmanual_search_term(params)
    elif params['ad_type'] == 'SP-ASIN':
        if params['ad_options'] == '广告位':
            query = data_aggregation_table_spasin_targeting_group(params)
        elif params['ad_options'] == '预算':
            query = data_aggregation_table_spasin(params)
        elif params['ad_options'] == '商品投放':
            query = data_aggregation_table_product_targets(params)
        elif params['ad_options'] == 'SKU':
            query = data_aggregation_table_spasin_sku(params)
        elif params['ad_options'] == '搜索词':
            query = data_aggregation_table_spasin_search_term(params)
    elif params['ad_type'] == 'SP-自动':
        if params['ad_options'] == '广告位':
            query = data_aggregation_table_spauto_targeting_group(params)
        elif params['ad_options'] == '预算':
            query = data_aggregation_table_spauto(params)
        elif params['ad_options'] == '自动定位组':
            query = data_aggregation_table_automatic_targeting(params)
        elif params['ad_options'] == 'SKU':
            query = data_aggregation_table_spauto_sku(params)
        elif params['ad_options'] == '搜索词':
            query = data_aggregation_table_spauto_search_term(params)
    elif params['ad_type'] == 'SD':
        if params['ad_options'] == '预算':
            query = data_aggregation_table_sd(params)
        elif params['ad_options'] == '内容投放':
            query = data_aggregation_table_sd_product_targets(params)
        elif params['ad_options'] == 'SKU':
            query = data_aggregation_table_sd_sku(params)
    else:
        return None
    #print(params['period'])
    order_query = order_query_method(params, params['period'])
    click_query = click_query_method(params, params['period'])
    impression_query = impression_query_method(params, params['period'])
    query_query = query_query_method(params)
    final_query = query + (order_query or "") + (click_query or "") + (impression_query or "") + (query_query or "")
    print(final_query)
    api1 = AmazonMysqlRagUitl(brand,params['country'])
    if params['price_adjustment_operation'] == '增加':
        bid_adjust = float(params['price_adjustment'])
    elif params['price_adjustment_operation'] == '减少':
        bid_adjust = -float(params['price_adjustment'])
    else:
        bid_adjust = None
    csv_filename = api1.get_select_campaign(final_query, params['cur_time'], bid_adjust)
    return csv_filename


