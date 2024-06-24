SET @session_fixed_date = '{}';
SET @session_fixed_country = '{}';
SELECT placementClassification,
    campaignName,
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 2 day) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 2 day) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 2 day) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 2 day) AND CURDATE() THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 2 day) AND CURDATE() THEN sales14d ELSE 0 END) AS ACOS_3d,
               SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END) AS ACOS_7d,
     SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

FROM
amazon_campaign_placement_reports_sp
  WHERE
    DATE BETWEEN '2024-04-27'
    AND (@session_fixed_date-INTERVAL 1 DAY)
    AND market = @session_fixed_country
    AND  campaignId in (select campaignId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=@session_fixed_date)
    AND( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
    and campaignId not in ( SELECT DISTINCT entityId
        FROM amazon_advertising_change_history
        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        AND market = @session_fixed_country
        AND predefinedTarget <> '')

GROUP BY
  campaignName,
  placementClassification
ORDER BY
  campaignName,
  placementClassification;
