def data_aggregation_table_spmanual(queries=None):
    # SQL查询模板
    query = f"""
    SELECT * FROM (
SELECT
                acr.campaignId,
                acr.campaignName,
                SUM( CASE WHEN acr.date = DATE_SUB( '{queries['cur_time']}', INTERVAL 1 DAY ) THEN acr.campaignBudgetAmount ELSE 0 END ) AS Budget,
                acr.market,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
                amazon_campaign_reports_sp acr
                JOIN amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
        WHERE
                acr.date BETWEEN DATE_SUB( '{queries['cur_time']}', INTERVAL 30 DAY )
                AND ( '{queries['cur_time']}' - INTERVAL 1 DAY )
                AND acr.campaignId IN ( SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{queries['cur_time']}' - INTERVAL 1 DAY)
                AND acr.campaignId NOT IN ( SELECT DISTINCT campaignId FROM amazon_targeting_reports_sp WHERE matchType = 'TARGETING_EXPRESSION' )
                AND acl.targetingType LIKE '%MAN%' -- 这里筛选手动广告
                AND acr.market = '{queries['country']}'
        GROUP BY
                acr.campaignId
								) AS a
	WHERE
	market = '{queries['country']}'
    """

    return query

def data_aggregation_table_spasin(queries=None):
    # SQL查询模板
    query = f"""
    SELECT * FROM (
SELECT
                acr.campaignId,
                acr.campaignName,
                SUM( CASE WHEN acr.date = DATE_SUB( '{queries['cur_time']}', INTERVAL 1 DAY ) THEN acr.campaignBudgetAmount ELSE 0 END ) AS Budget,
                acr.market,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
                amazon_campaign_reports_sp acr
                JOIN amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
        WHERE
                acr.date BETWEEN DATE_SUB( '{queries['cur_time']}', INTERVAL 30 DAY )
                AND ( '{queries['cur_time']}' - INTERVAL 1 DAY )
                AND acr.campaignId IN ( SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{queries['cur_time']}' - INTERVAL 1 DAY)
                AND acr.campaignId IN ( SELECT DISTINCT campaignId FROM amazon_targeting_reports_sp WHERE matchType = 'TARGETING_EXPRESSION' )
                AND acl.targetingType LIKE '%MAN%' -- 这里筛选手动广告
                AND acr.market = '{queries['country']}'
        GROUP BY
                acr.campaignId
								) AS a
	WHERE
	market = '{queries['country']}'
    """

    return query

def data_aggregation_table_spauto(queries=None):
    # SQL查询模板
    query = f"""
    SELECT * FROM (
SELECT
                acr.campaignId,
                acr.campaignName,
                SUM( CASE WHEN acr.date = DATE_SUB( '{queries['cur_time']}', INTERVAL 1 DAY ) THEN acr.campaignBudgetAmount ELSE 0 END ) AS Budget,
                acr.market,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{queries['cur_time']}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{queries['cur_time']}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
                amazon_campaign_reports_sp acr
                JOIN amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
        WHERE
                acr.date BETWEEN DATE_SUB( '{queries['cur_time']}', INTERVAL 30 DAY )
                AND ( '{queries['cur_time']}' - INTERVAL 1 DAY )
                AND acr.campaignId IN ( SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{queries['cur_time']}' - INTERVAL 1 DAY)
                AND acl.targetingType LIKE '%AUT%' -- 这里筛选手动广告
                AND acr.market = '{queries['country']}'
        GROUP BY
                acr.campaignId
								) AS a
	WHERE
	market = '{queries['country']}'
    """

    return query

def order_query_method(queries,period):
    if queries['order_count_operation'] != '无':
        if queries['order_count_operation'] == '大于':
            if period == "3":
                query = f''' AND total_order_3d > {queries['order_count']} '''
            elif period == "7":
                query = f''' AND total_order_7d > {queries['order_count']} '''
            elif period == "30":
                query = f''' AND total_order_30d > {queries['order_count']} '''
        elif queries['order_count_operation'] == '小于':
            if period == "3":
                query = f''' AND total_order_3d < {queries['order_count']} '''
            elif period == "7":
                query = f''' AND total_order_7d < {queries['order_count']} '''
            elif period == "30":
                query = f''' AND total_order_30d < {queries['order_count']} '''
        return query
    else:
        return None

def click_query_method(queries,period):
    if queries['click_count_operation'] != '无':
        if queries['click_count_operation'] == '大于':
            if period == "3":
                query = f''' AND total_clicks_3d > {queries['click_count']} '''
            elif period == "7":
                query = f''' AND total_clicks_7d > {queries['click_count']} '''
            elif period == "30":
                query = f''' AND total_clicks_30d > {queries['click_count']} '''
        elif queries['click_count_operation'] == '小于':
            if period == "3":
                query = f''' AND total_clicks_3d < {queries['click_count']} '''
            elif period == "7":
                query = f''' AND total_clicks_7d < {queries['click_count']} '''
            elif period == "30":
                query = f''' AND total_clicks_30d < {queries['click_count']} '''
        return query
    else:
        return None

def impression_query_method(queries,period):
    if queries['impression_operation'] != '无':
        if queries['impression_operation'] == '大于':
            if period == "3":
                query = f''' AND total_impressions_3d > {queries['impression_count']} '''
            elif period == "7":
                query = f''' AND total_impressions_7d > {queries['impression_count']} '''
            elif period == "30":
                query = f''' AND total_impressions_30d > {queries['impression_count']} '''
        elif queries['impression_operation'] == '小于':
            if period == "3":
                query = f''' AND total_impressions_3d < {queries['impression_count']} '''
            elif period == "7":
                query = f''' AND total_impressions_7d < {queries['impression_count']} '''
            elif period == "30":
                query = f''' AND total_impressions_30d < {queries['impression_count']} '''
        return query
    else:
        return None

def query_query_method(queries):
    if queries['query_field_operation'] != '无':
        if queries['query_field_operation'] == 'LIKE':
            query = f''' AND campaignName LIKE '%{queries['query_field_value']}%' '''
        elif queries['query_field_operation'] == 'NOT LIKE':
            query = f''' AND campaignName NOT LIKE '%{queries['query_field_value']}%' '''
        return query
    else:
        return None
