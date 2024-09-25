class TargetingGroupQueryManual:
    def get_query_v1_0(self,cur_time, country):
        query = """
                SELECT placementClassification,
                    campaignName,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                FROM
                amazon_campaign_placement_reports_sp
                WHERE
                    DATE BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
                    AND ('{}'-INTERVAL 1 DAY)
                    AND market = '{}'
                    AND  campaignId in (select campaignId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=DATE_SUB('{}', INTERVAL 1 DAY))
                    AND( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )

                GROUP BY
                  campaignName,
                  placementClassification
                ORDER BY
                  campaignName,
                  placementClassification;
                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, country, cur_time)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = """
                                SELECT placementClassification,
                                    campaignName,
                                    campaignId,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

                                FROM
                                amazon_campaign_placement_reports_sp
                                WHERE
                                    DATE BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
                                    AND ('{}'-INTERVAL 1 DAY)
                                    AND market = '{}'
                                    AND  campaignId in (select campaignId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=DATE_SUB('{}', INTERVAL 1 DAY))
                                    AND( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )

                                GROUP BY
                                  campaignName,
                                  placementClassification
                                ORDER BY
                                  campaignName,
                                  placementClassification;
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                                   cur_time)
        return query

    def get_query_v1_2(self,cur_time, country):
        query = """
                                SELECT
            a.campaignName,
            a.campaignId,
                        a.placementClassification,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday ,
            COALESCE(
                CASE
                    WHEN a.placementClassification = 'Detail Page on-Amazon' THEN c.dynamicBidding_placementProductPage_percentage
                    WHEN a.placementClassification = 'Other on-Amazon' THEN c.dynamicBidding_placementRestOfSearch_percentage
                    WHEN a.placementClassification = 'Top of Search on-Amazon' THEN c.dynamicBidding_placementTop_percentage
                END,
            0) AS bid
        FROM
            amazon_campaign_placement_reports_sp a
        JOIN
            (SELECT
                 campaignId,
                 targetingType,
                 dynamicBidding_placementTop_percentage,
                 dynamicBidding_placementProductPage_percentage,
                 dynamicBidding_placementRestOfSearch_percentage
             FROM
                 amazon_campaigns_list_sp
             ) c ON a.campaignId = c.campaignId
        WHERE
            a.market = '{}'
            AND a.campaignId IN (
                SELECT campaignId
                FROM amazon_campaigns_list_sp
                WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                AND campaignName NOT LIKE '%_overstock%'
            )
            AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
        GROUP BY
            a.campaignName,
            a.campaignId,
            a.placementClassification,
            c.dynamicBidding_placementTop_percentage,
            c.dynamicBidding_placementProductPage_percentage,
            c.dynamicBidding_placementRestOfSearch_percentage
        ORDER BY
            a.campaignName,
            a.placementClassification;
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, country, cur_time)
        return query

    def get_query_v1_3(self,cur_time, country):
        query = """
                                SELECT
            a.campaignName,
            a.campaignId,
                        a.placementClassification,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday ,
            COALESCE(
                CASE
                    WHEN a.placementClassification = 'Detail Page on-Amazon' THEN c.dynamicBidding_placementProductPage_percentage
                    WHEN a.placementClassification = 'Other on-Amazon' THEN c.dynamicBidding_placementRestOfSearch_percentage
                    WHEN a.placementClassification = 'Top of Search on-Amazon' THEN c.dynamicBidding_placementTop_percentage
                END,
            0) AS bid
        FROM
            amazon_campaign_placement_reports_sp a
        JOIN
            (SELECT
                 campaignId,
                 targetingType,
                 dynamicBidding_placementTop_percentage,
                 dynamicBidding_placementProductPage_percentage,
                 dynamicBidding_placementRestOfSearch_percentage
             FROM
                 amazon_campaigns_list_sp
             ) c ON a.campaignId = c.campaignId
        WHERE
            a.market = '{}'
            AND a.campaignId IN (
                SELECT campaignId
                FROM amazon_campaigns_list_sp
                WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
            )
            AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
        GROUP BY
            a.campaignName,
            a.campaignId,
            a.placementClassification,
            c.dynamicBidding_placementTop_percentage,
            c.dynamicBidding_placementProductPage_percentage,
            c.dynamicBidding_placementRestOfSearch_percentage
        ORDER BY
            a.campaignName,
            a.placementClassification;
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, country, cur_time)
        return query

