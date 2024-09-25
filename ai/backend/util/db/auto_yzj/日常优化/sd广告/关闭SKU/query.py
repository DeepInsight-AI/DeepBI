class SkuQuerySD:
    def get_query_v1_0(self,cur_time, country):
        query = """
        SELECT
            a.adGroupName,                             -- 广告组名称
            a.adId,                                -- 广告ID
            a.campaignId,                          -- 广告活动ID
            a.campaignName,                       -- 广告活动名称
            a.promotedSku AS advertisedSku,        -- 使用 promotedSku 代替 advertisedSku
            -- 计算过去30天内的总订单数，使用 purchases 字段，因为它代表 "Number of attributed conversion events occurring within 14 days of an ad click or view."
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_1m,
            -- 计算过去7天内的总订单数，使用 purchases 字段
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_7d,
            -- 计算过去30天内（包含今天）的总点击量
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
            -- 计算过去7天内（包含今天）的总点击量
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
            -- 计算昨天的总点击量
            SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
            -- 计算过去30天内的总销售额，使用 sales 字段，因为它代表 "Total value of sales occurring within 14 days of an ad click or view."
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_30d,
            -- 计算过去7天内的总销售额，使用 sales 字段
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_7d,
            -- 计算昨天的总销售额，使用 sales 字段
            SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) AS total_sales_yesterday,
            -- 计算过去30天内的总成本
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
            -- 计算过去7天内的总成本
            SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
            -- 计算昨天的总成本
            SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
            -- 计算过去30天内的平均广告花费回报率 (ACOS)，使用 sales 字段
            CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0
                 THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                      SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END)
                 ELSE 0
            END AS ACOS_30d,
            -- 计算过去7天内的平均广告花费回报率 (ACOS)，使用 sales 字段
            CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0
                 THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                      SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END)
                 ELSE 0
            END AS ACOS_7d,
            -- 计算昨天的平均广告花费回报率 (ACOS)，使用 sales 字段
            CASE WHEN SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) > 0
                 THEN SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) /
                      SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END)
                 ELSE 0
            END AS ACOS_yesterday
        -- 从 amazon_advertised_product_reports_sd 表中读取数据，并将其连接到 amazon_campaigns_list_sd 表
        FROM
            amazon_advertised_product_reports_sd a
        JOIN
            amazon_campaigns_list_sd c ON a.campaignId = c.campaignId
        -- 设置查询条件
        WHERE
            a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}' - INTERVAL 1 DAY  -- 筛选过去30天内的数据
            AND a.market = '{}'
            AND a.campaignId IN (
                SELECT campaignId
                FROM amazon_advertised_product_reports_sd
                WHERE date = '{}' - INTERVAL 1 DAY
            )
            AND (a.campaignName LIKE '%0507%' OR a.campaignName LIKE '%0509%') -- 筛选广告活动名称包含'0507'或'0509' --修改: 添加筛选条件
        -- 根据广告组名称、广告ID、广告活动名称和广告商品SKU对结果进行分组
        GROUP BY
            adGroupName,
            a.adId,
            a.campaignName,
            advertisedSku
        -- 按照广告组名称、广告活动名称和广告商品SKU对结果进行排序
        ORDER BY
            adGroupName,
            a.campaignName,
            advertisedSku;
                                                                    """.format(cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time,
                                                                               cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time, cur_time, country,
                                                                               cur_time)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = f"""
WITH DailyData AS (
    SELECT
        b.campaignName,
        b.campaignId,
        b.adGroupName,
        b.adGroupId,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases ELSE 0 END) AS ad_total_purchases_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS ad_total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ad_total_sales_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases ELSE 0 END) AS ad_total_purchases_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS ad_total_clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS ad_total_sales_7d
    FROM
        amazon_advertised_product_reports_sd b
    JOIN
        amazon_campaigns_list_sd c ON b.campaignId = c.campaignId
    WHERE
        b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}' - INTERVAL 1 DAY
        AND b.market = '{country}'
        AND b.campaignId IN (
            SELECT DISTINCT campaignId
            FROM amazon_advertised_product_reports_sd
            WHERE date = '{cur_time}' - INTERVAL 1 DAY
              AND market = '{country}'
              AND (campaignName LIKE '%0507%' OR campaignName LIKE '%0509%')
        )
    GROUP BY
        b.campaignId,
        b.adGroupName,
        b.adGroupId
),
list1 AS (
    SELECT
        a.adGroupName,
        a.adId,
        a.campaignId,
        a.adGroupId,
        a.campaignName,
        a.promotedSku AS advertisedSku,
        a.promotedAsin,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_1m,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_7d,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_30d,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_7d,
        SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) AS total_sales_yesterday,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
        SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
        CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0 THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) ELSE 0 END AS ACOS_30d,
        CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0 THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) ELSE 0 END AS ACOS_7d,
        CASE WHEN SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) > 0 THEN SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) ELSE 0 END AS ACOS_yesterday,
        p.state AS sku_state
    FROM
        amazon_advertised_product_reports_sd a
    JOIN
        amazon_campaigns_list_sd c ON a.campaignId = c.campaignId
    LEFT JOIN
        amazon_productads_list_sd p ON a.promotedSku = p.sku AND a.campaignId = p.campaignId
    WHERE
        a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}' - INTERVAL 1 DAY
        AND a.market = '{country}'
        AND a.campaignId IN (
            SELECT DISTINCT campaignId
            FROM amazon_advertised_product_reports_sd
            WHERE date = '{cur_time}' - INTERVAL 1 DAY
              AND market = '{country}'
              AND (campaignName LIKE '%0507%' OR campaignName LIKE '%0509%')
        )
    GROUP BY
        a.campaignId,
        a.adGroupName,
        a.adGroupId,
        a.adId,
        a.campaignName,
        a.promotedSku,
        a.promotedAsin
)
SELECT
    list1.*,
    ad_total_purchases_30d,
    ad_total_clicks_30d,
    ad_total_sales_30d,
    ad_total_purchases_7d,
    ad_total_clicks_7d,
    ad_total_sales_7d
FROM
    list1
LEFT JOIN
    DailyData z ON list1.campaignId = z.campaignId AND  list1.adGroupId = z.adGroupId
WHERE
    list1.sku_state != 'paused'
                        """
        return query
