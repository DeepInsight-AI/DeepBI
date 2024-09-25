class ProductTargetsQuery:
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
                -- 过去30天的总订单数
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                -- 过去30天的总点击量
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                -- 过去7天的总点击量
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                -- 昨天的总点击量
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                -- 过去30天的总销售额
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                -- 过去7天的总销售额
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                -- 昨天的总销售额
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                -- 过去30天的总成本
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                -- 过去7天的总成本
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                -- 过去4天的总成本
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                -- 昨天的总成本
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                -- 过去30天的平均成本销售比（ACOS）
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                -- 过去7天的平均成本销售比（ACOS）
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                -- 昨天的平均成本销售比（ACOS）
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS ACOS_yesterday
            FROM
                amazon_targeting_reports_sp b
            JOIN
                amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
            WHERE
                b.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}' - INTERVAL 1 DAY
                AND b.market = '{}'
                AND b.keywordId IN (
                    SELECT keywordId
                    FROM amazon_targeting_reports_sp
                    WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                )
                AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
                -- 排除最近4天内有变更的keywordId
                AND b.keywordId NOT IN (SELECT DISTINCT entityId
                    FROM amazon_advertising_change_history
                    WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                    AND entityType = 'KEYWORD'
                    AND market = '{}')
                AND b.campaignId NOT IN (SELECT DISTINCT campaignId FROM amazon_targeting_reports_sd) -- 排除在amazon_targeting_reports_sd中的campaignId
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
        -- 从CTE结果中选择数据
        SELECT
            a.*,
            d.keywordBid
        FROM
            a
        LEFT JOIN
            amazon_targeting_reports_sp d ON a.keywordId = d.keywordId
        WHERE
            d.date = DATE_SUB('{}', INTERVAL 1 DAY)
            AND (a.keyword LIKE '%asin%' OR a.targeting LIKE '%asin%' OR a.keyword LIKE '%category%' OR a.targeting LIKE '%category%' OR a.campaignName LIKE '%ASIN%');
                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, country, cur_time, country, cur_time)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = f"""
    SELECT
        b.keywordId,
        b.keyword,
        b.targeting,
        b.matchType,
        b.adGroupName,
        b.campaignName,
        c.bid,
        -- 过去30天的总订单数
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
        -- 过去30天的总点击量
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        -- 过去7天的总点击量
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        -- 昨天的总点击量
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
        -- 过去30天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
        -- 过去7天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
        -- 过去3天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
        -- 昨天的总销售额
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
        -- 过去30天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        -- 过去7天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        -- 过去3天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
        -- 昨天的总成本
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        -- 过去30天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
        -- 过去7天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
         -- 过去3天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
        -- 昨天的平均成本销售比（ACOS）
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS ACOS_yesterday
    FROM
        amazon_targeting_reports_sp b
                LEFT JOIN
                                amazon_targets_list_sp c ON b.keywordId = c.targetId
    WHERE
        b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}' - INTERVAL 1 DAY
        AND b.market = '{country}'
                                AND b.campaignId IN (
        SELECT DISTINCT campaignId
        FROM amazon_campaigns_list_sp
        WHERE state = 'ENABLED'
        AND market = '{country}'
    )
        AND b.keywordId NOT IN (SELECT DISTINCT entityId
            FROM amazon_advertising_change_history
            WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
            AND entityType = 'PRODUCT_TARGETING'
                                                AND productTargetingType = 'EXPRESSION'
            AND market = '{country}')
                                AND matchType in ('TARGETING_EXPRESSION')
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

                                """
        return query



