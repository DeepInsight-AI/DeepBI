SELECT
    adGroupName,
    campaignName,
    advertisedSku,
    -- 过去30天（包含今天）的总点击量
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_30d,
    -- 过去7天（包含今天）的总点击量
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN clicks ELSE 0 END) AS total_clicks_7d,
    -- 昨天的总点击量
    SUM(CASE WHEN date = CURDATE() - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
    -- 过去30天的总销售额
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_30d,
    -- 过去7天的总销售额
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END) AS total_sales14d_7d,
    -- 昨天的总销售额
    SUM(CASE WHEN date = CURDATE() - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
    -- 过去30天的总成本
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_30d,
    -- 过去7天的总成本
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) AS total_cost_7d,
    -- 昨天的总成本
    SUM(CASE WHEN date = CURDATE() - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
    -- 过去30天的平均成本销售比（ACOS）
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 29 day) AND CURDATE() THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
    -- 过去7天的平均成本销售比（ACOS）
    SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB(@session_fixed_date - INTERVAL 1 DAY, INTERVAL 6 DAY) AND CURDATE() THEN sales14d ELSE 0 END), 0) AS ACOS_7d,
    -- 昨天的平均成本销售比（ACOS）
    SUM(CASE WHEN date = CURDATE() - INTERVAL 2 DAY THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date = CURDATE() - INTERVAL 2 DAY THEN sales14d ELSE 0 END), 0) AS ACOS_yesterday
FROM
    amazon_advertised_product_reports_sp
WHERE
    date BETWEEN '2024-04-26' AND (@session_fixed_date-INTERVAL 1 DAY)
    AND market = @session_fixed_country
    AND adId IN (
        SELECT adId
        FROM amazon_advertised_product_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = @session_fixed_date
    )
    AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
    AND campaignId NOT IN (
        SELECT DISTINCT entityId
        FROM amazon_advertising_change_history
        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        AND market = @session_fixed_country
        AND predefinedTarget <> ''
    )
GROUP BY
    adGroupName,
    campaignName,
    advertisedSku
ORDER BY
    adGroupName,
    campaignName,
    advertisedSku;
