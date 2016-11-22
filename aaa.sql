CREATE TEMPORARY FUNCTION abtpop AS 'com.criteo.hadoop.hive.udf.UDFABTestPopulation';
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET hive.exec.parallel = true;


SET test_start_users = '2016-10-07';
SET test_start_measures = '2016-10-14';
SET test_end = '2016-11-15';

SELECT
    event.user_id
    , event.partner_id
    , CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id}) = 0
        THEN 'exposed' ELSE 'control' END AS population
    , NULL AS first_day_seen
    , 0 AS events
    , 0 AS clicks
    , 0.0 AS spend
    , COUNT(DISTINCT event.transaction_id) AS transactions
    , SUM(event.order_value) AS order_value
    , 0 as displays
FROM
    (
        SELECT
            user_id
            , partner_id
            , transaction_id
            , unixtime
            , SUM(b.price * b.quantity) AS order_value
        FROM
            bi_data.partnerdb_bi_advertiser_event
                LATERAL VIEW explode(products) p AS b
        WHERE
            partner_id IN (12629, 12364)
            AND partner_partition IN (pmod(12629, 1000), pmod(12364, 1000))
            AND day >= ${hiveconf:test_start_measures}
            AND day <= ${hiveconf:test_end}
            AND user_id <> '00000000-0000-0000-0000-000000000000'
            AND persistent_user = TRUE
            AND event_name = 'Sales'
        GROUP BY
            user_id
            , partner_id
            , transaction_id
            , unixtime
    ) event
JOIN
    (
        SELECT
            user_id
            , partner_id
            , transaction_id
            , MAX(unixtime) AS max_ts
        FROM
            bi_data.partnerdb_bi_advertiser_event
        WHERE
            partner_id IN (12629, 12364)
            AND partner_partition IN (pmod(12629, 1000), pmod(12364, 1000))
            AND day >= ${hiveconf:test_start_measures}
            AND day <= ${hiveconf:test_end}
            AND user_id <> '00000000-0000-0000-0000-000000000000'
            AND persistent_user = TRUE
            AND event_name = 'Sales'
        GROUP BY
            user_id
            , partner_id
            , transaction_id
    ) max_time
ON event.user_id = max_time.user_id
    AND event.partner_id = max_time.partner_id
    AND event.transaction_id = max_time.transaction_id
    AND event.unixtime = max_time.max_ts
GROUP BY
    event.user_id
    , event.partner_id
    , CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id}) = 0
        THEN 'exposed' ELSE 'control' END AS population
