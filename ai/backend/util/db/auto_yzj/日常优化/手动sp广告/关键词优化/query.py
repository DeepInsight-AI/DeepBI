class KeywordQuery:
    def get_query_v1_0(self,cur_time, country):
        query = """
                    WITH a AS (
            SELECT
                keywordId,
                keyword,
                targeting,
                matchType,
                adGroupName,
                campaignName,
                -- ... 其他字段和聚合计算
                                          SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                       SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
             SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
            FROM
                amazon_targeting_reports_sp
            WHERE
                date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) and DATE_SUB('{}', INTERVAL 1 DAY)
                -- ... 其他 WHERE 条件
                                        AND market = '{}'
            -- 确保keywordId是来自特定日期的启用状态的campaign
            AND keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}'- INTERVAL 1 DAY)
            -- 确保campaignName包含特定文本
            AND (campaignName LIKE '%MAN%' OR campaignName LIKE '%手动%' OR campaignName LIKE '%Man%' OR campaignName LIKE '%man%')
            -- 排除最近4天内有变更的keywordId
            AND keywordId NOT IN (
                SELECT DISTINCT entityId
                FROM amazon_advertising_change_history
                WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                AND entityType = 'KEYWORD'
                AND market = '{}')
            GROUP BY
                adGroupName,
                campaignName,
                keyword,
                matchType,
                keywordId,
                targeting
            ORDER BY
                adGroupName,
                campaignName,
                keyword,
                matchType,
                keywordId,
                targeting)
        SELECT
            a.*,
            b.keywordBid
        FROM
            a
        LEFT JOIN
            amazon_targeting_reports_sp b ON a.keywordId = b.keywordId
        WHERE
            b.date = DATE_SUB('{}', INTERVAL 1 DAY)
                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                   cur_time, country, cur_time)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = """
                        WITH a AS (
            SELECT
                keywordId,
                keyword,
                targeting,
                matchType,
                adGroupName,
                campaignName,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
            FROM
                amazon_targeting_reports_sp b
            JOIN
            amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
            WHERE
            b.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
            AND b.market = '{}'
            AND b.keywordId IN (
                SELECT keywordId
                FROM amazon_targeting_reports_sp
                WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
            )
            AND c.targetingType like '%MAN%'
            AND  b.keywordId not in (SELECT DISTINCT entityId
                FROM amazon_advertising_change_history
                WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                AND entityType = 'KEYWORD'
                AND market = '{}')
            GROUP BY
                b.adGroupName,
                b.campaignName,
                b.keyword,
                b.matchType,
                b.targeting,
                b.keywordId
            ORDER BY
                b.adGroupName,
                b.campaignName,
                b.keyword,
                b.matchType,
                b.keywordId
                )
        SELECT
            a.*,
            d.keywordBid
        FROM
            a
        LEFT JOIN
            amazon_targeting_reports_sp d ON a.keywordId = d.keywordId
        WHERE
            d.date = DATE_SUB('{}', INTERVAL 1 DAY)
                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, country, cur_time, country, cur_time)
        return query

    def get_query_v1_2(self,cur_time, country):
        query = """
                                    WITH a AS (
                        SELECT
                            keywordId,
                            keyword,
                            targeting,
                            matchType,
                            adGroupName,
                            campaignName,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                        FROM
                            amazon_targeting_reports_sp b
                        JOIN
                        amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                        WHERE
                        b.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                        AND b.market = '{}'
                        AND b.keywordId IN (
                            SELECT keywordId
                            FROM amazon_targeting_reports_sp
                            WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                        )
                        AND c.targetingType like '%MAN%'
                        AND  b.keywordId not in (SELECT DISTINCT entityId
                            FROM amazon_advertising_change_history
                            WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                            AND entityType = 'KEYWORD'
                            AND market = '{}')
                        AND matchType not in ('TARGETING_EXPRESSION')
                        GROUP BY
                            b.adGroupName,
                            b.campaignName,
                            b.keyword,
                            b.matchType,
                            b.targeting,
                            b.keywordId
                        ORDER BY
                            b.adGroupName,
                            b.campaignName,
                            b.keyword,
                            b.matchType,
                            b.keywordId
                            )
                    SELECT
                        a.*,
                        d.keywordBid
                    FROM
                        a
                    LEFT JOIN
                        amazon_targeting_reports_sp d ON a.keywordId = d.keywordId
                    WHERE
                        d.date = DATE_SUB('{}', INTERVAL 1 DAY)
                                    """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                               cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                               cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                               cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                               cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                               cur_time, cur_time, cur_time, cur_time, country, cur_time, country,
                                               cur_time)
        return query

    def get_query_v1_3(self,cur_time, country):
        query = f"""
WITH a AS (
    SELECT
        keywordId,
        keyword,
        targeting,
        matchType,
        adGroupName,
        campaignName,
        -- ... 其他字段和聚合计算
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
         SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
    FROM
        amazon_targeting_reports_sp b
   JOIN
    amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
WHERE
    b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}'- INTERVAL 1 DAY
    AND b.market = '{country}'
    AND b.keywordId IN (
        SELECT keywordId
        FROM amazon_targeting_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{cur_time}' - INTERVAL 1 DAY
    )
    AND c.targetingType like '%MAN%'  -- 筛选出手动广告
AND matchType not in ('TARGETING_EXPRESSION')
    -- 排除最近4天内有变更的keywordId
    AND  b.keywordId not in (SELECT DISTINCT entityId
        FROM amazon_advertising_change_history
        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        AND entityType = 'KEYWORD'
        AND market = '{country}')
    GROUP BY
        b.adGroupName,
        b.campaignName,
        b.keyword,
        b.matchType,
        b.targeting,
        b.keywordId
    ORDER BY
        b.adGroupName,
        b.campaignName,
        b.keyword,
        b.matchType,
        b.keywordId
        ),
z AS(
SELECT
        campaignName,
        b.campaignId,
        adGroupName,
        b.adGroupId,
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ad_total_purchases7d_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS ad_total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ad_total_sales14d_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ad_total_purchases7d_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS ad_total_clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ad_total_sales14d_7d
        FROM
        amazon_advertised_product_reports_sp b
    JOIN
        amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
    -- JOIN        amazon_sp_productads_list p on a.campaignId = p.campaignId and a.advertisedSku = p.sku
    WHERE
        b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}'- INTERVAL 1 DAY
        AND b.market = '{country}'
       AND c.targetingType like '%MAN%' -- 筛选出手动广告


    GROUP BY
        adGroupName,
        campaignName
    ORDER BY
        adGroupName,
        campaignName
)
SELECT
    a.*,
                z.ad_total_purchases7d_30d,
                z.ad_total_clicks_30d,
                z.ad_total_sales14d_30d,
                z.ad_total_purchases7d_7d,
                z.ad_total_clicks_7d,
                z.ad_total_sales14d_7d,
    d.keywordBid
FROM
    a
LEFT JOIN
    amazon_targeting_reports_sp d ON a.keywordId = d.keywordId
                LEFT JOIN
    z ON z.adGroupId = d.adGroupId
WHERE
    d.date = DATE_SUB('{cur_time}', INTERVAL 1 DAY)
                                                """
        return query


