--- Set enviroment variables ---
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET hive.exec.parallel = TRUE;

--- Set query variables ---
SET your_database = cluo;
SET partnerid = (3642, 3641, 5802, 3640, 3643);
SET partnerparti = (642, 641, 802, 640, 643);
SET start_date = '2016-11-07';
SET end_date = '2016-11-13';

--- Query ---
USE ${hiveconf:your_database};
DROP TABLE IF EXISTS CTR_CR_hour_2;

CREATE TABLE CTR_CR_hour_2 AS
SELECT
    d.merchant_id AS partner_id
    , d.user_id
    , d.day
    , d.hour_since_last_visit
    , d.total_displays
    , d.FCB_displays
    , c.clicks
    , c.rev_euro
    , c.rev_local
    , s.sales
FROM hours_since_last_visit_raw d
        --- Clicks ---
        LEFT OUTER JOIN
        (
            SELECT  c.merchant_id
                    , c.user_id
                    , c.day
                    , COUNT(1) AS clicks
                    , SUM(revenue_local) AS rev_local
                    , SUM(revenue_euro) AS rev_euro
                FROM bi_data.bi_click c
                LEFT SEMI JOIN hours_since_last_visit_raw u
                    ON c.user_id = u.user_id
                    AND c.merchant_id = u.merchant_id
                    AND c.day = u.day
                WHERE
                    c.merchant_id IN ${hiveconf:partnerid}
                    AND c.day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
                    AND c.user_id <> '00000000-0000-0000-0000-000000000000'
                    AND host_platform = "EU"
                    AND days_last_visit <= 1
                    AND days_last_visit > 0
                GROUP BY c.merchant_id
                    , c.user_id
                    , c.day
        ) c ON d.user_id = c.user_id
            AND d.merchant_id = c.merchant_id
            AND d.day = c.day

        --- Sales / Order value ---
        LEFT OUTER JOIN
        (
            SELECT  s.partner_id
                    , s.user_id
                    , s.day
                    , COUNT(1) AS sales
                FROM bi_data.partnerdb_matched_transactions s
                LEFT SEMI JOIN hours_since_last_visit_raw u
                    ON s.user_id = u.user_id
                    AND s.partner_id = u.merchant_id
                WHERE
                    partner_id IN ${hiveconf:partnerid}
                    AND day >= ${hiveconf:start_date}
                    AND day <= ${hiveconf:end_date}
                GROUP BY
                    s.partner_id
                    , s.user_id
                    , s.day
        ) s ON d.user_id = s.user_id
            AND d.merchant_id = s.partner_id
            AND d.day = s.day
