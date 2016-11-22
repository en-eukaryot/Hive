SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
-- SET sort.io.mb=512;
-- SET hive.enforce.bucketing = true;
SET hive.cli.print.header = true;

SET startDate = '2016-07-01';
SET endDate = '2016-09-30';
SET trans_enddate = "2016-10-30";
SET partnerId = 2429;

SELECT
    CASE WHEN uid.matched_cookies >= 3 THEN 3
        WHEN uid.matched_cookies >= 2 THEN 2
        ELSE 1 END AS matched_cookies,
    COUNT(DISTINCT user_id) AS total_cookies,
    SUM(num_disp) AS displays,
    SUM(clicks) AS clicks,
    SUM(spend) AS spend,
    SUM(transactions) AS transactions,
    SUM(order_value) AS order_value
FROM
    (
        SELECT
            impression_id,
            user_id,
            1 AS num_disp
        FROM
            bi_data.partnerdb_bi_display
        WHERE
            DAY BETWEEN ${hiveconf:startDate} AND ${hiveconf:endDate}
            AND partner_id = ${hiveconf:partnerId}
            AND partner_partition = pmod(${hiveconf:partnerId}, 1000)
        GROUP BY
            impression_id,
            user_id
    ) disp
LEFT OUTER JOIN(
        SELECT
            impression_id,
            COALESCE(item_id, 0) AS item_id,
            COUNT(1) AS clicks,
            SUM(revenue_local) AS spend
        FROM
            bi_data.partnerdb_bi_click
        WHERE
            DAY BETWEEN ${hiveconf:startDate} AND ${hiveconf:endDate}
            AND partner_id = ${hiveconf:partnerId}
            AND partner_partition = pmod(${hiveconf:partnerId}, 1000)
        GROUP BY
            impression_id ,
            COALESCE(item_id, 0)
    ) clk ON clk.impression_id = disp.impression_id
LEFT OUTER JOIN(
        SELECT
            impression_id ,
            COALESCE(clicked_item_id, 0) AS clicked_item_id ,
            COUNT(DISTINCT transaction_id) AS transactions ,
            SUM(order_value) AS order_value
        FROM
            bi_data.cpop_matched_transactions_rich
        WHERE
            partner_id = ${hiveconf:partnerId}
            AND DAY BETWEEN ${hiveconf:startDate} AND ${hiveconf:trans_enddate}
            AND attribution_type = 'pc'
        GROUP BY
            impression_id ,
            COALESCE(clicked_item_id, 0)
    ) mt ON mt.clicked_item_id = clk.item_id AND mt.impression_id = clk.impression_id
LEFT OUTER JOIN(
        SELECT
            uid ,
            COUNT(DISTINCT my_events.uid) AS matched_cookies
        FROM
            bi_data.uid_matching_full
                LATERAL VIEW EXPLODE(events) x AS my_events
        WHERE
            host_platform = 'EU'
            AND DAY = '2016-09-30'
        GROUP BY
            uid,
            users
    ) uid ON disp.user_id = uid.uid
GROUP BY
    CASE WHEN uid.matched_cookies >= 3 THEN 3
        WHEN uid.matched_cookies >= 2 THEN 2
        ELSE 1 END
;
