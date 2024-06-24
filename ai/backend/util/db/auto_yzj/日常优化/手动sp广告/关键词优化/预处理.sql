SET @session_fixed_date = '{}';
SET @session_fixed_country = '{}';
SELECT keywordId,keyword,targeting,keywordBid,matchType,adGroupName,
    campaignName,
                 SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN purchases7d ELSE 0 END) AS ORDER_1m,
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 3 DAY) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_4d,
                SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales14d ELSE 0 END) AS ACOS_30d,
               SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END) AS ACOS_7d,
     SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = @session_fixed_date - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

FROM
 amazon_targeting_reports_sp
  WHERE
    DATE BETWEEN '2024-04-28'
    AND (@session_fixed_date-INTERVAL 1 DAY)
    AND market = @session_fixed_country
    AND  keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=@session_fixed_date)
    AND ( campaignName LIKE '%MAN%' OR campaignName LIKE '%手动%' OR campaignName LIKE '%Man%' OR campaignName LIKE '%man%' )
    and  keywordId not in  (SELECT
DISTINCT entityId
FROM
amazon_advertising_change_history
WHERE
timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
and entityType = 'KEYWORD'
and market = @session_fixed_country )

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
        targeting;
