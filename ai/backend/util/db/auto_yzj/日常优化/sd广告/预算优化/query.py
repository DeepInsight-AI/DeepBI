class BudgetQuerySD:
    def get_query_v1_0(self,cur_time, country):
        query = """
        WITH CampaignStats AS (
            SELECT
                a.campaignId,                          -- 广告活动ID
                a.campaignName,                       -- 广告活动名称
                c.budget AS campaignBudget,           -- 预算
                a.market,                           -- 市场

                -- 计算昨天的花费
                SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.cost ELSE 0 END) AS costYesterday,

                -- 计算昨天的点击量
                SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.clicks ELSE 0 END) AS clicksYesterday,

                -- 计算昨天的销售额，使用 sales 字段，因为它代表 "Total value of sales occurring within 14 days of an ad click or view."
                SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.sales ELSE 0 END) AS salesYesterday,

                -- 计算过去7天的总花费
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS totalCost7d,

                -- 计算过去7天的总销售额，使用 sales 字段
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS totalSales7d,

                -- 计算过去30天的总花费
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS totalCost30d,

                -- 计算过去30天的总销售额，使用 sales 字段
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS totalSales30d,

                -- 计算过去30天的总点击量
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS totalClicks30d,

                -- 计算过去7天的总点击量
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS totalClicks7d,

                -- 计算过去30天的ACOS (Advertising Cost of Sales)，使用 sales 字段
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END), 0) AS ACOS30d,

                -- 计算过去7天的ACOS (Advertising Cost of Sales)，使用 sales 字段
                SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END), 0) AS ACOS7d,

                -- 计算昨天的ACOS (Advertising Cost of Sales)，使用 sales 字段
                SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.sales ELSE 0 END), 0)  AS ACOSYesterday

            FROM
                amazon_advertised_product_reports_sd a
            JOIN
                amazon_campaigns_list_sd c ON a.campaignId = c.campaignId
            WHERE
                -- 筛选过去30天内的数据
                a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}' - INTERVAL 1 DAY)

                -- 筛选在查询日期前一天仍然处于启用状态的广告活动
                AND a.campaignId IN (
                    SELECT campaignId
                    FROM amazon_advertised_product_reports_sd
                    WHERE date = '{}' - INTERVAL 1 DAY
                )

                -- 筛选法国市场的数据
                AND a.market = '{}'

            -- 根据广告活动ID、名称、预算和市场进行分组
            GROUP BY
                a.campaignId,
                a.campaignName,
                c.budget,
                a.market
        ),

        -- 计算每个市场的平均 ACOS
        CountryAvgACOS AS (
            SELECT
                SUM(reports.cost) / SUM(reports.sales) AS countryAvgACOS1m,
                reports.market
            FROM
                amazon_advertised_product_reports_sd AS reports
            INNER JOIN
                amazon_campaigns_list_sd AS campaigns ON reports.campaignId = campaigns.campaignId
            WHERE
                reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}' - INTERVAL 1 DAY)
                AND campaigns.campaignId IN (
                    SELECT campaignId
                    FROM amazon_advertised_product_reports_sd
                    WHERE date = '{}' - INTERVAL 1 DAY
                )
                AND reports.market = '{}'
            GROUP BY
                reports.market
        )

        -- 连接两个 CTE，获取最终结果并筛选campaignName包含0507和0509的数据
        SELECT
            cs.*,
            ca.countryAvgACOS1m
        FROM
            CampaignStats cs
        JOIN
            CountryAvgACOS ca ON cs.market = ca.market
        WHERE
            cs.campaignName LIKE '%0507%' OR cs.campaignName LIKE '%0509%';

                                                                    """.format(cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time, cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time, cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time, cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               cur_time, cur_time,
                                                                               cur_time, cur_time, cur_time, cur_time,
                                                                               country, cur_time, cur_time,
                                                                               cur_time, country)
        return query
