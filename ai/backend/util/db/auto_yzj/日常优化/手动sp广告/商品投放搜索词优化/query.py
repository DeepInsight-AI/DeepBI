class ProductTargetsSearchTermQuery:
    def get_query_v1_0(self,cur_time, country):
        query = """
        SELECT
            a.keyword,
            a.searchTerm,
            a.adGroupName,
            a.adGroupId,
            a.matchType,
            a.campaignName,
            a.campaignId,
            -- 过去30天的总点击量
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
            -- 过去7天的总点击量
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
            -- 昨天的总点击量
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
            -- 过去30天的总销售额
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
            -- 过去7天的总销售额
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
            -- 昨天的总销售额
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
            -- 过去30天的总成本
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
            -- 过去7天的总成本
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
            -- 昨天的总成本
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
            -- 过去30天的平均成本销售比（ACOS）
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
            -- 过去7天的平均成本销售比（ACOS）
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
            -- 昨天的平均成本销售比（ACOS）
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) /
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS ACOS_yesterday,
            -- 过去30天的总订单数
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
            -- 过去7天的总订单数
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d
        FROM
            amazon_search_term_reports_sp a
        JOIN
            amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
        WHERE
            a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}' - INTERVAL 1 DAY
            AND a.market = '{}'
            AND a.keywordId IN (
                SELECT keywordId
                FROM amazon_targeting_reports_sp
                WHERE campaignStatus = 'ENABLED'
                AND date = '{}' - INTERVAL 1 DAY
                AND campaignName NOT LIKE '%_overstock%'
            )
            AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
            AND (
                a.keyword LIKE '%asin%'
                OR a.targeting LIKE '%asin%'
                OR a.campaignName LIKE '%ASIN%'
                OR a.keyword LIKE '%category%'
                OR a.targeting LIKE '%category%'
            ) -- 筛选出keyword、targeting中包含asin或category，或campaignName中包含ASIN的数据行
            AND a.campaignId NOT IN (
                SELECT DISTINCT campaignId
                FROM amazon_targeting_reports_sd
            ) -- 排除在 amazon_targeting_reports_sd 中的 campaignId
        GROUP BY
            a.adGroupName,
            a.campaignName,
            a.keyword,
            a.searchTerm,
            a.matchType
        ORDER BY
            a.adGroupName,
            a.campaignName,
            a.keyword,
            a.searchTerm;
                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time,
                                   cur_time, country, cur_time)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = """
                SELECT
                    a.keyword,
                    a.searchTerm,
                    a.adGroupName,
                    a.adGroupId,
                    a.matchType,
                    a.campaignName,
                    a.campaignId,
                    -- 过去30天的总点击量
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                    -- 过去7天的总点击量
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                    -- 昨天的总点击量
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                    -- 过去30天的总销售额
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
                    -- 过去7天的总销售额
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
                    -- 昨天的总销售额
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
                    -- 过去30天的总成本
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                    -- 过去7天的总成本
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                    -- 昨天的总成本
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                    -- 过去30天的平均成本销售比（ACOS）
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
                    -- 过去7天的平均成本销售比（ACOS）
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
                    -- 昨天的平均成本销售比（ACOS）
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) /
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS ACOS_yesterday,
                    -- 过去30天的总订单数
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                    -- 过去7天的总订单数
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d
                FROM
                    amazon_search_term_reports_sp a
                JOIN
                    amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                WHERE
                    a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}' - INTERVAL 1 DAY
                    AND a.market = '{}'
                    AND a.keywordId IN (
                        SELECT keywordId
                        FROM amazon_targeting_reports_sp
                        WHERE campaignStatus = 'ENABLED'
                        AND date = '{}' - INTERVAL 1 DAY
                    )
                    AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
                    AND matchType in ('TARGETING_EXPRESSION')
                GROUP BY
                    a.adGroupName,
                    a.campaignName,
                    a.searchTerm,
                    a.matchType
                ORDER BY
                    a.adGroupName,
                    a.campaignName,
                    a.searchTerm;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time,
                                           cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time,
                                           cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time,
                                           cur_time, cur_time,
                                           cur_time, country, cur_time)
        return query

    def get_query_v1_2(self,cur_time, country):
        query = f"""
SELECT
    a.keyword,
    a.searchTerm,
    a.adGroupName,
    a.adGroupId,
    a.matchType,
    a.campaignName,
    a.campaignId,
    -- 过去30天的总点击量
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
    -- 过去7天的总点击量
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    -- 昨天的总点击量
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    -- 过去30天的总销售额
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
    -- 过去7天的总销售额
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
    -- 昨天的总销售额
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
    -- 过去30天的总成本
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
    -- 过去7天的总成本
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
    -- 昨天的总成本
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    -- 过去30天的平均成本销售比（ACOS）
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
    -- 过去7天的平均成本销售比（ACOS）
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
    -- 昨天的平均成本销售比（ACOS）
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) /
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS ACOS_yesterday,
    -- 过去30天的总订单数
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
    -- 过去7天的总订单数
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS CPC_30d
FROM
    amazon_search_term_reports_sp a
WHERE
    a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}' - INTERVAL 1 DAY
    AND a.campaignName LIKE 'DeepBI_%'
    AND a.market = '{country}'
    AND a.campaignId IN (
        SELECT DISTINCT campaignId
        FROM amazon_campaigns_list_sp
        WHERE state = 'ENABLED'
        AND market = '{country}'
    )
    AND matchType in ('TARGETING_EXPRESSION')
GROUP BY
    a.adGroupName,
    a.campaignName,
    a.searchTerm
ORDER BY
    a.adGroupName,
    a.campaignName,
    a.searchTerm;
                                    """
        return query


