class reopenSkuQuerySD:
    def get_query_v1_0(self,cur_time, country):
        query = f"""
        SELECT *
        FROM (
            SELECT
                a.adGroupName,                             -- 广告组名称
                a.adId,                                -- 广告ID
                a.campaignId,                          -- 广告活动ID
                a.campaignName,                       -- 广告活动名称
                a.promotedSku AS advertisedSku,        -- 使用 promotedSku 代替 advertisedSku
                a.promotedAsin,
                -- 计算过去30天内的总订单数，使用 purchases 字段，因为它代表 "Number of attributed conversion events occurring within 14 days of an ad click or view."
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_1m,
                -- 计算过去7天内的总订单数，使用 purchases 字段
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_7d,
                -- 计算过去30天内（包含今天）的总点击量
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                -- 计算过去7天内（包含今天）的总点击量
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                -- 计算昨天的总点击量
                SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                -- 计算过去30天内的总销售额，使用 sales 字段，因为它代表 "Total value of sales occurring within 14 days of an ad click or view."
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_30d,
                -- 计算过去7天内的总销售额，使用 sales 字段
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_7d,
                -- 计算昨天的总销售额，使用 sales 字段
                SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) AS total_sales_yesterday,
                -- 计算过去30天内的总成本
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                -- 计算过去7天内的总成本
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                -- 计算昨天的总成本
                SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                -- 计算过去30天内的平均广告花费回报率 (ACOS)，使用 sales 字段
                CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0
                     THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                          SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END)
                     ELSE 0
                END AS ACOS_30d,
                -- 计算过去7天内的平均广告花费回报率 (ACOS)，使用 sales 字段
                CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0
                     THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                          SUM(CASE WHEN a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END)
                     ELSE 0
                END AS ACOS_7d,
                -- 计算昨天的平均广告花费回报率 (ACOS)，使用 sales 字段
                CASE WHEN SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) > 0
                     THEN SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) /
                          SUM(CASE WHEN a.date = '{cur_time}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END)
                     ELSE 0
                END AS ACOS_yesterday,
                p.state AS sku_state  -- 添加 sku_state 列
            -- 从 amazon_advertised_product_reports_sd 表中读取数据，并将其连接到 amazon_campaigns_list_sd 表
            FROM
                amazon_advertised_product_reports_sd a
            JOIN
                amazon_campaigns_list_sd c ON a.campaignId = c.campaignId
            LEFT JOIN  -- 使用 LEFT JOIN，即使没有匹配的记录也能获取到已有的数据
                amazon_productads_list_sd p ON a.promotedSku = p.sku AND a.campaignId=p.campaignId   -- 连接条件：promotedSku 和 sku 相匹配，market 也要匹配
            -- 设置查询条件
            WHERE
                a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}' - INTERVAL 1 DAY  -- 筛选过去30天内的数据
                AND a.market = '{country}'
                AND a.campaignId IN (
                    SELECT campaignId
                    FROM amazon_advertised_product_reports_sd
                    WHERE date = '{cur_time}' - INTERVAL 1 DAY
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
                advertisedSku
        ) AS list1
        WHERE list1.sku_state = 'paused';

                                                                    """
        return query

