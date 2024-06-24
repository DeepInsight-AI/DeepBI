SET @session_fixed_date = '{}';
SET @session_fixed_country = '{}';
WITH Campaign_Stats AS (
    SELECT
        campaignName,
        SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 day) AND CURDATE() THEN cost ELSE 0 END) AS cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 day) AND CURDATE() THEN sales14d ELSE 0 END) AS sales14d_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN cost ELSE 0 END) AS cost_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales14d ELSE 0 END) AS sales14d_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales7d ELSE 0 END) AS sales_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 day) AND CURDATE() THEN clicks ELSE 0 END) AS clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN clicks ELSE 0 END) AS clicks_1m
    FROM
        amazon_campaign_reports_sp
    WHERE
        date BETWEEN '2024-04-28' AND (@session_fixed_date-INTERVAL 1 DAY)
        AND campaignStatus = 'ENABLED'
        AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
        AND market = @session_fixed_country
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
    (SELECT SUM(cost) / SUM(sales14d) FROM amazon_campaign_reports_sp WHERE market = @session_fixed_country AND date BETWEEN '2024-04-28' AND (@session_fixed_date-INTERVAL 1 DAY) AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )) AS country_avg_ACOS_1m
FROM
    amazon_campaign_reports_sp a
LEFT JOIN Campaign_Stats cs ON a.campaignName = cs.campaignName
WHERE
    a.date = (@session_fixed_date-INTERVAL 1 DAY)
    AND a.campaignStatus = 'ENABLED'
    AND ( a.campaignName not LIKE '%AUTO%' and  a.campaignName not  LIKE '%auto%' and a.campaignName not LIKE '%Auto%' and  a.campaignName not LIKE '%自动%' )
    AND a.market = @session_fixed_country
ORDER BY
    a.date;
