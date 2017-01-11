-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 1024;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET click_startdate = "2016-09-01";
SET click_enddate = "2016-11-15";
SET trans_enddate = "2016-12-15";
SET partnerid = (4155, 4154);

SELECT
    a.days_last_visit,
    COUNT(1) as nb_clicks,
    SUM(a.nb_transactions) as nb_sales
FROM
    (
    SELECT
        c.`timestamp`,
        c.user_id,
        CAST(c.days_last_visit AS INT) as days_last_visit,
        COUNT(distinct t.transaction_id) as nb_transactions
    FROM
        bi_data.bi_click c
    LEFT OUTER JOIN
    (
        SELECT
            user_id,
            transaction_id,
            click_timestamp
        FROM
            bi_data.cpop_matched_transactions_rich
        WHERE
            partner_id IN ${hiveconf:partnerid}
            AND attribution_type = 'pc'
            AND deduplication_matching = 1
            AND day BETWEEN ${hiveconf:click_startdate} AND ${hiveconf:trans_enddate}
            AND user_id <> '00000000-0000-0000-0000-000000000000'
    ) t
    ON c.user_id = t.user_id AND c.`timestamp` = t.click_timestamp

    WHERE
        c.merchant_id IN ${hiveconf:partnerid}
        AND c.host_platform ='EU'
        AND c.day BETWEEN ${hiveconf:click_startdate} AND ${hiveconf:click_enddate}
        AND c.user_id <> '00000000-0000-0000-0000-000000000000'

    GROUP BY
        c.`timestamp`,
        c.user_id,
        CAST(c.days_last_visit AS INT)
    ) a
GROUP BY
    a.days_last_visit
