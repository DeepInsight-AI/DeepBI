class SearchTermQueryManual:
    def get_query_v1_0(self,cur_time, country):
        query = """
        SELECT keyword,searchTerm,adGroupName,matchType,
            campaignName,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

        FROM
        amazon_search_term_reports_sp
        WHERE
        (date between DATE_SUB('{}', INTERVAL 30 DAY) and ('{}'-INTERVAL 1 DAY))
        and market='{}'
        and keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
        and  ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
        and campaignId not in (SELECT
        DISTINCT entityId
        FROM
        amazon_advertising_change_history
        WHERE
        timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        and market = '{}'
        and predefinedTarget <> '')
        GROUP BY
          adGroupName,
          campaignName,
          keyword,
          searchTerm,
                matchType
        ORDER BY
          adGroupName,
          campaignName,
          keyword,
          searchTerm;
                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, country, cur_time, country)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = """
                        SELECT keyword,searchTerm,adGroupName,adGroupId,matchType,
                            campaignName,
                            campaignId,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                        FROM
                        amazon_search_term_reports_sp
                        WHERE
                        (date between DATE_SUB('{}', INTERVAL 30 DAY) and ('{}'-INTERVAL 1 DAY))
                        and market='{}'
                        and keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}')
                        and  ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
                        and campaignId not in (SELECT
                        DISTINCT entityId
                        FROM
                        amazon_advertising_change_history
                        WHERE
                        timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                        and market = '{}'
                        and predefinedTarget <> '')
                        GROUP BY
                          adGroupName,
                          campaignName,
                          keyword,
                          searchTerm,
                                matchType
                        ORDER BY
                          adGroupName,
                          campaignName,
                          keyword,
                          searchTerm;
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                                   cur_time, country)
        return query

    def get_query_v1_2(self,cur_time, country):
        query = """
                        SELECT
                        a.keyword,
                        a.searchTerm,
                        a.adGroupName,
                        a.adGroupId,
                        a.matchType,
                        a.campaignName,
                        a.campaignId,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN purchases7d ELSE 0 END) AS ORDER_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday

        FROM
        amazon_search_term_reports_sp a
        JOIN
            amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
        WHERE
            a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
            AND a.market = '{}'
                        and a.keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
            AND c.targetingType like '%MAN%' -- 筛选出手动广告
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
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                                   cur_time)
        return query

    def get_query_v1_3(self,cur_time, country):
        query = """
                                    SELECT
                                    a.keyword,
                                    a.searchTerm,
                                    a.adGroupName,
                                    a.adGroupId,
                                    a.matchType,
                                    a.campaignName,
                                    a.campaignId,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN purchases7d ELSE 0 END) AS ORDER_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday

                    FROM
                    amazon_search_term_reports_sp a
                    JOIN
                        amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                    WHERE
                        a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                        AND a.market = '{}'
                        and a.keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
                        AND c.targetingType like '%MAN%' -- 筛选出手动广告
                        AND (a.campaignName not LIKE '%%ASIN%%' and a.campaignName not LIKE '%%asin%%' and a.campaignName not LIKE '%%商品投放%%' and a.campaignName not LIKE '%%品类投放%%' and a.campaignName not LIKE '%%CATEGORY%%' and a.campaignName not LIKE '%%PRODUCT%%')
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
                                                    """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               country,
                                                               cur_time)
        return query

    def get_query_v1_4(self,cur_time, country):
        query = """
                                    SELECT
                                    a.keyword,
                                    a.searchTerm,
                                    a.adGroupName,
                                    a.adGroupId,
                                    a.matchType,
                                    a.campaignName,
                                    a.campaignId,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN purchases7d ELSE 0 END) AS ORDER_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday

                    FROM
                    amazon_search_term_reports_sp a
                    JOIN
                        amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                    WHERE
                        a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                        AND a.market = '{}'
                        and a.keywordId in (select keywordId from amazon_search_term_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
                        AND c.targetingType like '%MAN%' -- 筛选出手动广告
                        AND (a.campaignName not LIKE '%%ASIN%%' and a.campaignName not LIKE '%%asin%%' and a.campaignName not LIKE '%%商品投放%%' and a.campaignName not LIKE '%%品类投放%%' and a.campaignName not LIKE '%%CATEGORY%%' and a.campaignName not LIKE '%%PRODUCT%%')
                        AND a.campaignId NOT IN (
        		SELECT DISTINCT campaignId
        		FROM amazon_targeting_reports_sp
        		WHERE matchType = 'TARGETING_EXPRESSION'
        		)
                        AND NOT EXISTS (
                   SELECT 1
                   FROM amazon_keywords_list_sp
                   WHERE keywordText = a.searchTerm
                   AND campaignId = a.campaignId
                   AND adGroupId = a.adGroupId
                )
                    GROUP BY
                      a.adGroupName,
                      a.campaignName,
                      a.searchTerm,
                      a.matchType
                    ORDER BY
                      a.adGroupName,
                      a.campaignName,
                      a.searchTerm;
                                                    """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               country,
                                                               cur_time)
        return query

    def get_query_v1_5(self,cur_time, country):
        query = f"""
SELECT
    a.keyword,
    a.searchTerm,
    a.adGroupName,
    a.adGroupId,
    a.matchType,
    a.campaignName,
    a.campaignId,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS CPC_30d
FROM
amazon_search_term_reports_sp a
WHERE
    a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}'- INTERVAL 1 DAY
    AND a.market = '{country}'
   AND a.campaignId IN (
        SELECT DISTINCT campaignId
        FROM amazon_campaigns_list_sp
        WHERE state = 'ENABLED'
        AND market = '{country}'
    )
    AND matchType  IN ('PHRASE','BROAD','EXACT')
    AND NOT EXISTS (
        SELECT 1
        FROM amazon_keywords_list_sp
        WHERE keywordText = a.searchTerm
          AND campaignId = a.campaignId
          AND adGroupId = a.adGroupId
    )
GROUP BY
  a.adGroupName,
  a.campaignName,
  a.searchTerm,
  a.matchType
ORDER BY
  a.adGroupName,
  a.campaignName,
  a.searchTerm;
                                                """
        return query

    def get_query_v1_6(self,cur_time, country):
        query = f"""
SELECT
    a.keyword,
    a.searchTerm,
    a.adGroupName,
    a.adGroupId,
    a.matchType,
    a.campaignName,
    a.campaignId,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
    SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
    SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS CPC_30d
FROM
amazon_search_term_reports_sp a
WHERE
    a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}'- INTERVAL 1 DAY
    AND a.market = '{country}'
    AND a.campaignName LIKE 'DeepBI_%'
   AND a.campaignId IN (
        SELECT DISTINCT campaignId
        FROM amazon_campaigns_list_sp
        WHERE state = 'ENABLED'
        AND market = '{country}'
    )
    AND matchType  IN ('PHRASE','BROAD','EXACT')
    AND NOT EXISTS (
        SELECT 1
        FROM amazon_keywords_list_sp
        WHERE keywordText = a.searchTerm
          AND campaignId = a.campaignId
          AND adGroupId = a.adGroupId
    )
GROUP BY
  a.adGroupName,
  a.campaignName,
  a.searchTerm,
  a.matchType
ORDER BY
  a.adGroupName,
  a.campaignName,
  a.searchTerm;
                                                """
        return query


