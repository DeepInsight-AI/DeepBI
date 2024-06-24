SET @session_fixed_date = '{}';
SET @session_fixed_country = '{}';
SELECT
keyword,
keywordId,
targeting,
matchtype,
keywordBid,
adGroupName,
campaignName,
SUM(clicks) AS total_clicks,
SUM(sales7d) AS total_sales
from
amazon_targeting_reports_sp
where  keywordId not in  (SELECT
DISTINCT entityId
FROM
amazon_advertising_change_history
WHERE
timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
and entityType = 'KEYWORD'
and market = @session_fixed_country )   AND  keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='2024-05-30')
    AND ( campaignName LIKE '%MAN%' OR campaignName LIKE '%手动%' OR campaignName LIKE '%Man%' OR campaignName LIKE '%man%' )
    AND market = @session_fixed_country
   GROUP BY
   keyword,
         matchType,
         campaignName,
         adGroupName
