class ProductTargetsQuerySD:
    def get_query_v1_0(self,cur_time, country):
        query = f"""
        WITH a AS (
            SELECT
                b.market,
                b.targetingId,
                b.targetingExpression,
                b.targetingText,
                b.adGroupName,
                b.campaignName,
                b.campaignId,
                b.date,
                c.state,
                -- ... 其他字段和聚合计算
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases ELSE 0 END) AS ORDER_1m,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_3d,
                SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales ELSE 0 END) AS total_sales_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ACOS_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ACOS_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ACOS_3d,
                SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales ELSE 0 END)  AS ACOS_yesterday
            FROM
                amazon_targeting_reports_sd b
                        JOIN
            amazon_campaigns_list_sd c ON b.campaignId = c.campaignId
                        -- 联接广告活动表，获取广告活动状态
                        WHERE
            b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}'- INTERVAL 1 DAY  AND b.market = '{country}'
                        AND c.state = 'enabled'
            AND b.targetingId IN (
                    SELECT targetingId
                    FROM amazon_targeting_reports_sd
                    WHERE adKeywordStatus = 'ENABLED' AND date = '{cur_time}' - INTERVAL 1 DAY)
                                        GROUP BY
                b.market,
                b.targetingId
        )

        SELECT
            a.*,
            d.bid
        FROM
            a
        LEFT JOIN
            amazon_targets_list_sd d ON a.targetingId = d.targetId
        WHERE
            a.date = DATE_SUB('{cur_time}', INTERVAL 1 DAY)
            AND (a.campaignName LIKE '%0507%' OR a.campaignName LIKE '%0509%')
            AND a.targetingText NOT LIKE 'similar-product%';
                                                                    """
        return query

    def get_query_v1_1(self,cur_time, country):
        query = f"""
 SELECT
        b.market,
        b.targetingId,
        b.targetingExpression,
        b.targetingText,
        b.adGroupName,
        b.campaignName,
        b.campaignId,
        b.date,
        c.bid,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases ELSE 0 END) AS ORDER_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_3d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales ELSE 0 END) AS total_sales_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ACOS_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ACOS_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ACOS_3d,
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales ELSE 0 END)  AS ACOS_yesterday
        FROM
                amazon_targeting_reports_sd b
                LEFT JOIN amazon_targets_list_sd c ON b.targetingId = c.targetId -- 联接广告活动表，获取广告活动状态
        WHERE
                b.date BETWEEN DATE_SUB( '{cur_time}', INTERVAL 30 DAY )
                AND '{cur_time}' - INTERVAL 1 DAY
                AND b.market = '{country}'
                AND b.campaignId IN (
        SELECT DISTINCT campaignId
        FROM amazon_campaigns_list_sd
        WHERE state = 'enabled'
        AND market = '{country}'
    )
                AND b.targetingText NOT LIKE 'similar-product%'
                AND b.targetingId NOT IN ('-96551807758360')
                AND c.state = 'enabled'
                AND (b.campaignName LIKE '%0507%' OR b.campaignName LIKE '%0509%')
        GROUP BY
                b.market,
                b.targetingId
                                """
        return query



