from ai.backend.util.db.auto_process.amazon_api_demo.util.data_aggregation_table import data_aggregation_table_spmanual,data_aggregation_table_spasin,data_aggregation_table_spauto,order_query_method,click_query_method,impression_query_method,query_query_method
from ai.backend.util.db.auto_process.amazon_api_demo.db_tool.db_tool import AmazonMysqlRagUitl

def access_param(params,brand):
    if params['ad_type'] == 'SP-手动':
        query = data_aggregation_table_spmanual(params)
    elif params['ad_type'] == 'SP-ASIN':
        query = data_aggregation_table_spasin(params)
    elif params['ad_type'] == 'SP-自动':
        query = data_aggregation_table_spauto(params)
    else:
        return None
    #print(params['period'])
    order_query = order_query_method(params, params['period'])
    click_query = click_query_method(params, params['period'])
    impression_query = impression_query_method(params, params['period'])
    query_query = query_query_method(params)
    final_query = query + (order_query or "") + (click_query or "") + (impression_query or "") + (query_query or "")
    print(final_query)
    api1 = AmazonMysqlRagUitl(brand)
    csv_filename = api1.get_select_campaign(final_query,params['cur_time'])
    return csv_filename


