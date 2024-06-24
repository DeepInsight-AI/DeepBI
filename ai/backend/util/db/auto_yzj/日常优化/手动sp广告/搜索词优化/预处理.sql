SET @session_fixed_date = '{}';
SET @session_fixed_country = '{}';
SELECT keyword,searchTerm,adGroupName,matchType,
    campaignName,
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales14d ELSE 0 END) AS ACOS_30d,
               SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END) AS ACOS_7d,
     SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

FROM
amazon_search_term_reports_sp
WHERE
(date between '2024-04-26' and (@session_fixed_date-INTERVAL 1 DAY))
and market=@session_fixed_country
and keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=@session_fixed_date)
and  ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
and campaignId not in (SELECT
DISTINCT entityId
FROM
amazon_advertising_change_history
WHERE
timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
and market = @session_fixed_country
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
