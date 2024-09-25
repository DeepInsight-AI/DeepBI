class BudgetQueryAuto:
    def get_query_v1_0(self,cur_time, country):
        query = """
        WITH Campaign_Stats AS (
            SELECT
                campaignName,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS cost_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS sales14d_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS cost_1m,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS sales14d_1m,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales7d ELSE 0 END) AS sales_1m,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS clicks_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS clicks_1m
            FROM
                amazon_campaign_reports_sp
            WHERE
                date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                AND campaignStatus = 'ENABLED'
                AND ( campaignName  LIKE '%AUTO%' or  campaignName   LIKE '%auto%' or campaignName  LIKE '%Auto%' or  campaignName  LIKE '%自动%' )
                AND market = '{}'
            GROUP BY
                campaignName
        )
        SELECT
            a.campaignName,
            a.campaignId,
            a.date,
            a.campaignBudgetAmount AS Budget,
            a.clicks,
            a.cost,
            a.sales7d as sales,
            (a.cost / NULLIF(a.sales14d, 0)) AS ACOS,
            cs.sales_1m,
            cs.cost_1m,
            (cs.cost_7d / NULLIF(cs.sales14d_7d, 0)) AS avg_ACOS_7d,
            cs.cost_1m / NULLIF(cs.sales14d_1m, 0) AS avg_ACOS_1m,
            cs.clicks_1m,
            cs.clicks_7d,
            (SELECT SUM(cost) / SUM(sales14d) FROM amazon_campaign_reports_sp WHERE market = '{}' AND date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY) AND ( campaignName  LIKE '%AUTO%' or  campaignName   LIKE '%auto%' or campaignName  LIKE '%Auto%' or  campaignName  LIKE '%自动%' )) AS country_avg_ACOS_1m
        FROM
            amazon_campaign_reports_sp a
        LEFT JOIN Campaign_Stats cs ON a.campaignName = cs.campaignName
        WHERE
            a.date = ('{}'-INTERVAL 1 DAY)
            AND a.campaignStatus = 'ENABLED'
            AND ( a.campaignName  LIKE '%AUTO%' or  a.campaignName   LIKE '%auto%' or a.campaignName  LIKE '%Auto%' or  a.campaignName  LIKE '%自动%' )
            AND a.market = '{}'
        ORDER BY
            a.date;

                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   country, country, cur_time, cur_time, cur_time, country)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = """
                       WITH Campaign_Stats AS (
                SELECT
                    campaignId,
                    campaignName,
                    campaignBudgetAmount AS Budget,
                    market,
                    sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN cost ELSE 0 END) as cost_yesterday,
                    sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN clicks ELSE 0 END) as clicks_yesterday,
                    sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN sales14d ELSE 0 END) as sales_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN sales14d ELSE 0 END) AS ACOS_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN sales14d ELSE 0 END) AS ACOS_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                FROM
                amazon_campaign_reports_sp
                WHERE
                date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                AND campaignId IN (
                SELECT campaignId
                FROM amazon_campaign_reports_sp
                WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
                AND  ( campaignName LIKE '%AUTO%' or  campaignName  LIKE '%auto%' or campaignName LIKE '%Auto%' or  campaignName LIKE '%自动%' )
                AND market = '{}'
                GROUP BY
                campaignName
        ),
        b as (
                select sum(cost)/sum(sales14d) as country_avg_ACOS_1m,market
                from amazon_campaign_reports_sp
                WHERE
                date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                AND campaignId IN (
                SELECT campaignId
                FROM amazon_campaign_reports_sp
                WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
                AND  ( campaignName LIKE '%AUTO%' or  campaignName  LIKE '%auto%' or campaignName LIKE '%Auto%' or  campaignName LIKE '%自动%' )
                AND market = '{}'
                                        )
            SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
            from Campaign_Stats join b
                on Campaign_Stats.market =b.market

                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, country, cur_time, cur_time,
                                                   cur_time, country)
        return query

    def get_query_v1_2(self,cur_time, country):
        query = """
                                   WITH Campaign_Stats AS (
             SELECT
                acr.campaignId,
                acr.campaignName,
                acr.campaignBudgetAmount AS Budget,
                acr.market,
                sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) as cost_yesterday,
                sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) as clicks_yesterday,
                sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) as sales_yesterday,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d,
                SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.sales14d ELSE 0 END), 0)  AS ACOS_yesterday
            FROM
                amazon_campaign_reports_sp acr
            JOIN
                amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
            WHERE
                acr.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                AND acr.campaignId IN (
                    SELECT campaignId
                    FROM amazon_campaign_reports_sp
                    WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
                AND acl.targetingType LIKE '%AUT%'  -- 这里筛选手动广告
                AND acr.market = '{}'
            GROUP BY
                acr.campaignName
        ),
        b as (SELECT
            SUM(reports.cost)/SUM(reports.sales14d) AS country_avg_ACOS_1m,
            reports.market
        FROM
            amazon_campaign_reports_sp AS reports
        INNER JOIN
            amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
        WHERE
            reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                        and campaigns.campaignId in ( SELECT campaignId
                FROM amazon_campaign_reports_sp
             WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
             AND campaignName NOT LIKE '%_overstock%')

            AND campaigns.targetingType LIKE '%AUT%'  -- 筛选手动广告
            AND reports.market = '{}'
        GROUP BY
            reports.market)
          SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
                from Campaign_Stats join b
                on Campaign_Stats.market =b.market
                                                    """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               country,
                                                               cur_time, cur_time, cur_time, country)
        return query

    def get_query_v1_3(self,cur_time, country):
        query = """
                                   WITH Campaign_Stats AS (
             SELECT
                acr.campaignId,
                acr.campaignName,
                acr.campaignBudgetAmount AS Budget,
                acr.market,
                sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) as cost_yesterday,
                sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) as clicks_yesterday,
                sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) as sales_yesterday,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
                SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d,
                SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.sales14d ELSE 0 END), 0)  AS ACOS_yesterday
            FROM
                amazon_campaign_reports_sp acr
            JOIN
                amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
            WHERE
                acr.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                AND acr.campaignId IN (
                    SELECT campaignId
                    FROM amazon_campaign_reports_sp
                    WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
                AND acl.targetingType LIKE '%AUT%'  -- 这里筛选手动广告
                AND acr.market = '{}'
            GROUP BY
                acr.campaignName
        ),
        b as (SELECT
            SUM(reports.cost)/SUM(reports.sales14d) AS country_avg_ACOS_1m,
            reports.market
        FROM
            amazon_campaign_reports_sp AS reports
        INNER JOIN
            amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
        WHERE
            reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                        and campaigns.campaignId in ( SELECT campaignId
                FROM amazon_campaign_reports_sp
             WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )

            AND campaigns.targetingType LIKE '%AUT%'  -- 筛选手动广告
            AND reports.market = '{}'
        GROUP BY
            reports.market)
          SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
                from Campaign_Stats join b
                on Campaign_Stats.market =b.market
                                                    """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time, cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               cur_time,
                                                               cur_time, cur_time, cur_time, cur_time, cur_time,
                                                               country,
                                                               cur_time, cur_time, cur_time, country)
        return query
