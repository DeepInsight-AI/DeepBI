def data_aggregation_table_spmanual(queries=None):
    # SQL查询模板
    query = f"""
    WITH a AS (
	SELECT
		campaignId,
		campaignName,
		market,
		date,
		campaignBudgetAmount,
		clicks,
		purchases7d,
		impressions,
		cost,
		sales14d
FROM
amazon_campaign_reports_sp
WHERE
market = '{queries['country']}'
AND date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 30 DAY ) AND ( CURRENT_DATE - INTERVAL 1 DAY )
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_keywords_list_sp
WHERE market = '{queries['country']}'
AND keywordText NOT IN ('(_targeting_auto_)')
AND extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
)
),
b AS(
	SELECT
		campaignId,
		campaign_name AS campaignName,
		market,
		CURRENT_DATE - INTERVAL 1 DAY AS date,
		budget_budget AS campaignBudgetAmount,
		0 AS clicks,
		0 AS purchases7d,
		0 AS impressions,
		0 AS cost,
		0 AS sales14d
	FROM
	amazon_campaigns_list_sp
	WHERE
	market = '{queries['country']}'
	AND state = 'ENABLED'
	AND campaignId NOT IN ( SELECT DISTINCT campaignId FROM a
	)
	AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_keywords_list_sp
WHERE market = '{queries['country']}'
AND keywordText NOT IN ('(_targeting_auto_)')
AND extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
)
),
c AS (
SELECT * FROM a
UNION
SELECT * FROM b
),
d AS (
	SELECT
		campaignId,
		campaignName,
		market,
		SUM( CASE WHEN date = DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) THEN campaignBudgetAmount ELSE 0 END ) AS Budget,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
	FROM
	c
	GROUP BY
	campaignId
)
SELECT * FROM d
WHERE
market = '{queries['country']}'
    """

    return query

def data_aggregation_table_spasin(queries=None):
    # SQL查询模板
    query = f"""
    WITH a AS (
	SELECT
		campaignId,
		campaignName,
		market,
		date,
		campaignBudgetAmount,
		clicks,
		purchases7d,
		impressions,
		cost,
		sales14d
FROM
amazon_campaign_reports_sp
WHERE
market = '{queries['country']}'
AND date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 30 DAY ) AND ( CURRENT_DATE - INTERVAL 1 DAY )
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'MANUAL'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
b AS(
	SELECT
		campaignId,
		campaign_name AS campaignName,
		market,
		CURRENT_DATE - INTERVAL 1 DAY AS date,
		budget_budget AS campaignBudgetAmount,
		0 AS clicks,
		0 AS purchases7d,
		0 AS impressions,
		0 AS cost,
		0 AS sales14d
	FROM
	amazon_campaigns_list_sp
	WHERE
	market = '{queries['country']}'
	AND state = 'ENABLED'
	AND campaignId NOT IN ( SELECT DISTINCT campaignId FROM a
	)
	AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'MANUAL'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
c AS (
SELECT * FROM a
UNION
SELECT * FROM b
),
d AS (
	SELECT
		campaignId,
		SUM( CASE WHEN date = DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) THEN campaignName ELSE 0 END ) AS campaignName1,
		campaignName,
		market,
		SUM( CASE WHEN date = DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) THEN campaignBudgetAmount ELSE 0 END ) AS Budget,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
	FROM
	c
	GROUP BY
	campaignId
)
SELECT * FROM d
WHERE
market = '{queries['country']}'
    """

    return query

def data_aggregation_table_spauto(queries=None):
    # SQL查询模板
    query = f"""
WITH a AS (
	SELECT
		campaignId,
		campaignName,
		market,
		date,
		campaignBudgetAmount,
		clicks,
		purchases7d,
		impressions,
		cost,
		sales14d
FROM
amazon_campaign_reports_sp
WHERE
market = '{queries['country']}'
AND date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 30 DAY ) AND ( CURRENT_DATE - INTERVAL 1 DAY )
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'AUTO'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
b AS(
	SELECT
		campaignId,
		campaign_name AS campaignName,
		market,
		CURRENT_DATE - INTERVAL 1 DAY AS date,
		budget_budget AS campaignBudgetAmount,
		0 AS clicks,
		0 AS purchases7d,
		0 AS impressions,
		0 AS cost,
		0 AS sales14d
	FROM
	amazon_campaigns_list_sp
	WHERE
	market = '{queries['country']}'
	AND state = 'ENABLED'
	AND campaignId NOT IN ( SELECT DISTINCT campaignId FROM a
	)
	AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'AUTO'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
c AS (
SELECT * FROM a
UNION
SELECT * FROM b
),
d AS (
	SELECT
		campaignId,
		SUM( CASE WHEN date = DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) THEN campaignName ELSE 0 END ) AS campaignName1,
		campaignName,
		market,
		SUM( CASE WHEN date = DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) THEN campaignBudgetAmount ELSE 0 END ) AS Budget,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
		SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
	FROM
	c
	GROUP BY
	campaignId
)
SELECT * FROM d
WHERE
market = '{queries['country']}'
    """

    return query


def data_aggregation_table_spmanual_targeting_group(queries=None):
    # SQL查询模板
    query = f"""
WITH a AS (
SELECT
        acp.campaignId,
        acp.campaignName,
        acp.placementClassification,
        acp.market,
        acp.date,
        acp.clicks,
        acp.purchases7d,
        acp.impressions,
        acp.cost,
        acp.sales14d,
        COALESCE(
                        CASE
                                        WHEN acp.placementClassification = 'Detail Page on-Amazon' THEN acl.dynamicBidding_placementProductPage_percentage
                                        WHEN acp.placementClassification = 'Other on-Amazon' THEN acl.dynamicBidding_placementRestOfSearch_percentage
                                        WHEN acp.placementClassification = 'Top of Search on-Amazon' THEN acl.dynamicBidding_placementTop_percentage
                        END,
        0) AS bid
FROM amazon_campaign_placement_reports_sp acp
LEFT JOIN amazon_campaigns_list_sp acl ON acp.campaignId = acl.campaignId
WHERE acp.market = '{queries['country']}'
AND acp.date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 30 DAY ) AND ( CURRENT_DATE - INTERVAL 1 DAY )
AND acl.state = 'ENABLED'
AND acp.campaignId IN (
SELECT DISTINCT campaignId FROM amazon_keywords_list_sp
WHERE market = '{queries['country']}'
AND keywordText NOT IN ('(_targeting_auto_)')
AND extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
)
),
b AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Detail Page on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementRestOfSearch_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Detail Page on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_keywords_list_sp
WHERE market = '{queries['country']}'
AND keywordText NOT IN ('(_targeting_auto_)')
AND extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
)
),
c AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Other on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementProductPage_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Other on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_keywords_list_sp
WHERE market = '{queries['country']}'
AND keywordText NOT IN ('(_targeting_auto_)')
AND extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
)
),
d AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Top of Search on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementTop_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Top of Search on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_keywords_list_sp
WHERE market = '{queries['country']}'
AND keywordText NOT IN ('(_targeting_auto_)')
AND extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
)
),
e AS (
SELECT * FROM a
UNION
SELECT * FROM b
UNION
SELECT * FROM c
UNION
SELECT * FROM d
),
f AS (
SELECT
                campaignId,
                campaignName,
                placementClassification,
                market,
                bid,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
        e
        GROUP BY
        campaignId,
        placementClassification
        ORDER BY
        campaignId
)
SELECT * FROM f
WHERE
market = '{queries['country']}'
    """
    return query


def data_aggregation_table_spasin_targeting_group(queries=None):
    # SQL查询模板
    query = f"""
WITH a AS (
SELECT
        acp.campaignId,
        acp.campaignName,
        acp.placementClassification,
        acp.market,
        acp.date,
        acp.clicks,
        acp.purchases7d,
        acp.impressions,
        acp.cost,
        acp.sales14d,
        COALESCE(
                        CASE
                                        WHEN acp.placementClassification = 'Detail Page on-Amazon' THEN acl.dynamicBidding_placementProductPage_percentage
                                        WHEN acp.placementClassification = 'Other on-Amazon' THEN acl.dynamicBidding_placementRestOfSearch_percentage
                                        WHEN acp.placementClassification = 'Top of Search on-Amazon' THEN acl.dynamicBidding_placementTop_percentage
                        END,
        0) AS bid
FROM amazon_campaign_placement_reports_sp acp
LEFT JOIN amazon_campaigns_list_sp acl ON acp.campaignId = acl.campaignId
WHERE acp.market = '{queries['country']}'
AND acp.date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 30 DAY ) AND ( CURRENT_DATE - INTERVAL 1 DAY )
AND acl.state = 'ENABLED'
AND acp.campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'MANUAL'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
b AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Detail Page on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementRestOfSearch_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Detail Page on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'MANUAL'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
c AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Other on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementProductPage_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Other on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'MANUAL'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
d AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Top of Search on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementTop_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Top of Search on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'MANUAL'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
e AS (
SELECT * FROM a
UNION
SELECT * FROM b
UNION
SELECT * FROM c
UNION
SELECT * FROM d
),
f AS (
SELECT
                campaignId,
                campaignName,
                placementClassification,
                market,
                bid,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
        e
        GROUP BY
        campaignId,
        placementClassification
        ORDER BY
        campaignId
)
SELECT * FROM f
WHERE
market = '{queries['country']}'
    """
    return query


def data_aggregation_table_spauto_targeting_group(queries=None):
    # SQL查询模板
    query = f"""
WITH a AS (
SELECT
        acp.campaignId,
        acp.campaignName,
        acp.placementClassification,
        acp.market,
        acp.date,
        acp.clicks,
        acp.purchases7d,
        acp.impressions,
        acp.cost,
        acp.sales14d,
        COALESCE(
                        CASE
                                        WHEN acp.placementClassification = 'Detail Page on-Amazon' THEN acl.dynamicBidding_placementProductPage_percentage
                                        WHEN acp.placementClassification = 'Other on-Amazon' THEN acl.dynamicBidding_placementRestOfSearch_percentage
                                        WHEN acp.placementClassification = 'Top of Search on-Amazon' THEN acl.dynamicBidding_placementTop_percentage
                        END,
        0) AS bid
FROM amazon_campaign_placement_reports_sp acp
LEFT JOIN amazon_campaigns_list_sp acl ON acp.campaignId = acl.campaignId
WHERE acp.market = '{queries['country']}'
AND acp.date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 30 DAY ) AND ( CURRENT_DATE - INTERVAL 1 DAY )
AND acl.state = 'ENABLED'
AND acp.campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'AUTO'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
b AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Detail Page on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementRestOfSearch_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Detail Page on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'AUTO'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
c AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Other on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementProductPage_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Other on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'AUTO'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
d AS (
SELECT
        campaignId,
        campaign_name AS campaignName,
        'Top of Search on-Amazon' AS placementClassification,
        market,
        CURRENT_DATE - INTERVAL 1 DAY AS date,
        0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
        0 AS cost,
        0 AS sales14d,
        COALESCE(
        dynamicBidding_placementTop_percentage,
        0) AS bid
FROM
amazon_campaigns_list_sp
WHERE market = '{queries['country']}'
AND state = 'ENABLED'
AND campaignId NOT IN (
SELECT DISTINCT campaignId FROM a WHERE placementClassification = 'Top of Search on-Amazon')
AND campaignId IN (
SELECT DISTINCT campaignId FROM amazon_targets_list_sp
WHERE market = '{queries['country']}'
AND expressionType = 'AUTO'
AND servingStatus NOT IN ('CAMPAIGN_PAUSED','CAMPAIGN_ARCHIVED','AD_GROUP_ARCHIVED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED','TARGETING_CLAUSE_ARCHIVED')
)
),
e AS (
SELECT * FROM a
UNION
SELECT * FROM b
UNION
SELECT * FROM c
UNION
SELECT * FROM d
),
f AS (
SELECT
                campaignId,
                campaignName,
                placementClassification,
                market,
                bid,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
        e
        GROUP BY
        campaignId,
        placementClassification
        ORDER BY
        campaignId
)
SELECT * FROM f
WHERE
market = '{queries['country']}'
    """
    return query


def data_aggregation_table_keyword(queries=None):
    # SQL查询模板
    query = f"""
WITH a AS (
SELECT
        b.campaignName,
        b.campaignId,
        b.adGroupName,
        b.adGroupId,
        b.keyword,
        b.keywordId,
        b.matchType,
        b.market,
        date,
        clicks,
        purchases7d,
        impressions,
        cost,
        sales14d,
        bid
FROM
        amazon_targeting_reports_sp b
        JOIN amazon_keywords_list_sp c ON b.keywordId = c.keywordId
WHERE
        b.market = '{queries['country']}'
        AND b.date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 15 DAY )
        AND DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY )
                                ),
b AS (
SELECT
        NULL AS campaignName,
        akl.campaignId,
        NULL AS adGroupName,
        akl.adGroupId,
        akl.keywordText AS keyword,
        akl.keywordId,
        akl.matchType,
        market,
                                DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) AS date,
                                0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
                                0 AS cost,
                                0 AS sales14d,
        bid
FROM
        amazon_keywords_list_sp akl
WHERE
        akl.market = '{queries['country']}'
        AND akl.state = 'ENABLED'
        AND akl.extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
                                AND akl.keywordId NOT IN ( SELECT DISTINCT keywordId FROM a )
        ),
c AS (
SELECT * FROM a
UNION
SELECT * FROM b
),
d AS(
SELECT
        campaignName,
        campaignId,
        adGroupName,
        adGroupId,
        market,
                                keyword,
                                keywordId,
                                matchType,
                                bid,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
        c
        GROUP BY
        keywordId
        ORDER BY
        campaignId,
        adGroupId
)
SELECT * FROM d
WHERE
market = '{queries['country']}'
    """
    return query


def data_aggregation_table_product_targets(queries=None):
    # SQL查询模板
    query = f"""
WITH a AS (
    SELECT
        b.campaignName,
        b.campaignId,
        b.adGroupName,
        b.adGroupId,
        b.keyword,
        b.keywordId,
        b.market,
        date,
        clicks,
        purchases7d,
        impressions,
        cost,
        sales14d,
        bid
    FROM
        amazon_targeting_reports_sp b
        JOIN amazon_targets_list_sp c ON b.keywordId = c.targetId
    WHERE
        b.market = '{queries['country']}'
        AND b.date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 15 DAY)
        AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
        AND expressionType = 'MANUAL'
),
b AS (
    SELECT
        NULL AS campaignName,
        akl.campaignId,
        NULL AS adGroupName,
        akl.adGroupId,
            CASE
        WHEN expression LIKE "%'type': 'ASIN_SAME_AS', 'value': %" THEN CONCAT('asin="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(expression, "'", -2), "'", 1)), '"')
        WHEN expression LIKE "%'type': 'ASIN_EXPANDED_FROM', 'value': %" THEN CONCAT('asin-expanded="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(expression, "'", -2), "'", 1)), '"')
        WHEN expression LIKE "%'type': 'ASIN_CATEGORY_SAME_AS', 'value': %" THEN CONCAT('category="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(expression, "'", -2), "'", 1)), '"')
        WHEN expression LIKE "%'type': 'ASIN_CATEGORY_SAME_AS', 'value': %" AND expression LIKE "%, 'type': 'ASIN_BRAND_SAME_AS', 'value': %" THEN
            CONCAT('category="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(expression, "'", -2), "'", 2)), '" ',
                   'brand="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(expression, "'", -8), "'", 2)), '"')
        ELSE expression
    END AS keyword,
        akl.targetId AS keywordId,
        market,
        DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) AS date,
                                0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
                                0 AS cost,
                                0 AS sales14d,
        bid
    FROM
        amazon_targets_list_sp akl
    WHERE
        akl.market = '{queries['country']}'
        AND akl.state = 'ENABLED'
        AND akl.servingStatus NOT IN ('CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED')
        AND expressionType = 'MANUAL'
                                AND akl.targetId NOT IN (
                                        SELECT
                                                keywordId
                                        FROM
                                        a)
),
c AS (
SELECT * FROM a
UNION
SELECT * FROM b
),
d AS (
    SELECT
        campaignName,
        campaignId,
        adGroupName,
        adGroupId,
        market,
        keyword,
                                keywordId,
                                bid,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
        c
        GROUP BY
        keywordId
        ORDER BY
        campaignId,
        adGroupId
)
SELECT * FROM d
WHERE
market = '{queries['country']}'
    """
    return query


def data_aggregation_table_automatic_targeting(queries=None):
    # SQL查询模板
    query = f"""
WITH a AS (
SELECT
        b.campaignName,
        b.campaignId,
        b.adGroupName,
        b.adGroupId,
        b.keyword,
        b.keywordId,
        b.market,
        date,
        clicks,
        purchases7d,
        impressions,
        cost,
        sales14d,
        bid
FROM
        amazon_targeting_reports_sp b
        JOIN amazon_targets_list_sp c ON b.keywordId = c.targetId
WHERE
        b.market = '{queries['country']}'
        AND b.date BETWEEN DATE_SUB( CURRENT_DATE, INTERVAL 15 DAY )
        AND DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY )
                                AND expressionType='AUTO'
 ),
 b AS (
SELECT
        NULL AS campaignName,
        akl.campaignId,
        NULL AS adGroupName,
        akl.adGroupId,
         (CASE
        WHEN resolvedExpression like '%QUERY_HIGH_REL_MATCHES%' THEN 'close match'
        WHEN resolvedExpression like '%QUERY_BROAD_REL_MATCHES%' THEN 'loose match'
        WHEN resolvedExpression like '%ASIN_ACCESSORY_RELATED%' THEN 'complements'
        WHEN resolvedExpression like '%ASIN_SUBSTITUTE_RELATED%' THEN 'substitute'
    END) AS keyword,
        akl.targetId as keywordId,
        market,
        DATE_SUB( CURRENT_DATE, INTERVAL 1 DAY ) AS date,
                                0 AS clicks,
        0 AS purchases7d,
        0 AS impressions,
                                0 AS cost,
                                0 AS sales14d,
        bid
FROM
        amazon_targets_list_sp akl
WHERE
        akl.market = '{queries['country']}'
        AND akl.state = 'ENABLED'
        AND akl.servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
        AND expressionType='AUTO'
        AND akl.targetId NOT IN (
                SELECT DISTINCT
                        keywordId
                FROM
                        a
                )
),
c AS (
SELECT * FROM a
UNION
SELECT * FROM b
),
d AS(
SELECT
        campaignName,
        campaignId,
        adGroupName,
        adGroupId,
        market,
        keyword,
        keywordId,
        bid,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_3d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_3d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_order_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN impressions ELSE 0 END) AS total_impressions_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d
        FROM
        c
        GROUP BY
        keywordId
        ORDER BY
        campaignId,
        adGroupId
)
SELECT * FROM d
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
