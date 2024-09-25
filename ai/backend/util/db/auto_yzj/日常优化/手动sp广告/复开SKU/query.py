class reopenSkuQueryManual:
    def get_query_v1_0(self,cur_time, country):
        query = """
                               SELECT
                    adGroupName,
                    a.adId,
                    a.campaignId,
                    campaignName,
                    advertisedSku,
                        -- 过去30天的总订单数
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                        -- 过去7天的总订单数
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                        -- 过去30天（包含今天）的总点击量
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                        -- 过去7天（包含今天）的总点击量
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                        -- 昨天的总点击量
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                        -- 过去30天的总销售额
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                        -- 过去7天的总销售额
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                        -- 昨天的总销售额
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                        -- 过去30天的总成本
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                        -- 过去7天的总成本
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                        -- 昨天的总成本
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                        -- 过去30天的平均成本销售比（ACOS）
                        CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                             THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                  SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                             ELSE 0
                        END AS ACOS_30d,
                        -- 过去7天的平均成本销售比（ACOS）
                        CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                             THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                  SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                             ELSE 0
                        END AS ACOS_7d,
                        -- 昨天的平均成本销售比（ACOS）
                        CASE WHEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) > 0
                             THEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
                                  SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)
                             ELSE 0
                        END AS ACOS_yesterday
                    FROM
                        amazon_advertised_product_reports_sp a
                    JOIN
                        amazon_campaigns_list_sp c ON a.campaignId = c.campaignId
                    WHERE
                        a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                        AND a.market = '{}'
                        AND a.campaignId IN (
                            SELECT campaignId
                            FROM amazon_advertised_product_reports_sp
                            WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                            AND campaignName NOT LIKE '%_overstock%'
                        )
                        AND c.targetingType like '%MAN%'
                        AND not EXISTS (
                SELECT 1
                FROM amazon_sp_productads_list
                WHERE sku = a.advertisedSku
                  AND campaignId = a.campaignId
                  AND adId = a.adId
                  AND state in ('ARCHIVED','ENABLED')
            )
                    GROUP BY
                        adGroupName,
                        a.adId,
                        campaignName,
                        advertisedSku

                    ORDER BY
                        adGroupName,
                        campaignName,
                        advertisedSku;
                                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                           cur_time,
                                                           cur_time, cur_time,
                                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                           cur_time,
                                                           cur_time,
                                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                           cur_time,
                                                           cur_time, cur_time, cur_time, cur_time, cur_time,
                                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                           cur_time,
                                                           country, cur_time)
        return query

    def get_query_v1_1(self,cur_time, country):
        query = f"""
SELECT
    result1.*,  -- 选择 result1 中的所有列
    p.state  -- 选择 amazon_sp_productads_list 表中的 state 字段
FROM (
    -- 原始查询语句，计算各项指标
    SELECT
        adGroupName,
        a.adId,
        a.campaignId,
        c.campaign_name,
        advertisedSku,
        -- 过去30天的总订单数
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
        -- 过去7天的总订单数
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
        -- 过去30天（包含今天）的总点击量
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        -- 过去7天（包含今天）的总点击量
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        -- 昨天的总点击量
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
        -- 过去30天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
        -- 过去7天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
        -- 昨天的总销售额
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
        -- 过去30天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        -- 过去7天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        -- 昨天的总成本
        SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        -- 过去30天的平均成本销售比（ACOS）
        CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
             THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                  SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
             ELSE 0
        END AS ACOS_30d,
        -- 过去7天的平均成本销售比（ACOS）
        CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
             THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                  SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
             ELSE 0
        END AS ACOS_7d,
        -- 昨天的平均成本销售比（ACOS）
        CASE WHEN SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) > 0
             THEN SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
                  SUM(CASE WHEN date = '{cur_time}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)
             ELSE 0
        END AS ACOS_yesterday
    FROM
        amazon_advertised_product_reports_sp a
    JOIN
        amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
    WHERE
        a.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 30 DAY) AND '{cur_time}'- INTERVAL 1 DAY
        AND a.market = '{country}'
        AND a.campaignId IN (
          SELECT campaignId
            FROM amazon_campaigns_list_sp
            WHERE state = 'ENABLED'-- 选择广告状态为 'ENABLED'
       )    AND c.targetingType like '%MAN%' -- 筛选出自动广告

    GROUP BY
        adGroupName,
        a.adId, -- 对 adGroupName, adId, campaignName 和 advertisedSku 进行分组
        campaignName,
        advertisedSku

    ORDER BY
        adGroupName,
        campaignName,
        advertisedSku -- 按 adGroupName, campaignName 和 advertisedSku 进行排序
) AS result1 -- 为原始查询结果集指定别名
LEFT JOIN
    amazon_sp_productads_list p ON result1.campaignId = p.campaignId
                                    AND result1.advertisedSku = p.sku
                                    AND result1.adId = p.adId
                                    AND p.update_time = '{cur_time}'- INTERVAL 1 DAY   -- 添加 update_time 的筛选条件
WHERE p.state = 'PAUSED';

                        """
        return query

