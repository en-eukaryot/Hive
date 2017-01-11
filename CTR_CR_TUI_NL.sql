-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 1024;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET your_database = cluo;
SET partnerid = (4155, 4154);
SET partnerparti = (155, 154);
SET start_date = '2016-11-01';
SET end_date = '2016-11-30';

USE ${hiveconf:your_database};

--- Query ---
SELECT
    CASE
        WHEN avg_displays <= 2 THEN 'Max2'
        WHEN avg_displays <= 4 THEN 'Max4'
        WHEN avg_displays <= 6 THEN 'Max6'
        WHEN avg_displays <= 8 THEN 'Max8'
        WHEN avg_displays <= 10 THEN 'Max10'
        WHEN avg_displays <= 12 THEN 'Max12'
        WHEN avg_displays <= 14 THEN 'Max14'
        WHEN avg_displays <= 16 THEN 'Max16'
        WHEN avg_displays <= 18 THEN 'Max18'
        WHEN avg_displays <= 20 THEN 'Max20'
        WHEN avg_displays <= 22 THEN 'Max22'
        WHEN avg_displays <= 24 THEN 'Max24'
        ELSE 'Over24'
        END Avg_display_per_user_per_day
    , SUM(d.sum_displays) as sum_displays
    , CASE WHEN sum(c.clicks) is null then cast(0 as bigint) else sum(c.clicks) end as sum_clicks
    , CASE WHEN sum(s.sales) is null then cast(0 as bigint) else sum(s.sales) end as sum_sales
    , CASE WHEN sum(c.rev) is null then cast(0 as double) else sum(c.rev) end as rev
FROM (
        SELECT DISTINCT
                user_id
            FROM
                bi_data.partnerdb_bi_display
            WHERE
                partner_id IN ${hiveconf:partnerid}
                AND partner_partition IN ${hiveconf:partnerparti}
                AND day >= ${hiveconf:start_date}
                AND day <= ${hiveconf:end_date}
                AND user_id <> '00000000-0000-0000-0000-000000000000'
        ) u

        JOIN

        (
        SELECT
                user_id
                , AVG(total_displays) AS avg_displays
                , SUM(total_displays) AS sum_displays
            FROM frequency_raw_data
            GROUP BY
                user_id
        ) d ON u.user_id = d.user_id


        LEFT OUTER JOIN

        (
        SELECT  user_id
                , COUNT(1) AS clicks
                , SUM(revenue_euro) AS rev
            FROM bi_data.partnerdb_bi_click
            WHERE
                partner_id IN ${hiveconf:partnerid}
                AND partner_partition IN ${hiveconf:partnerparti}
                AND day >= ${hiveconf:start_date}
                AND day <= ${hiveconf:end_date}
                AND user_id <> '00000000-0000-0000-0000-000000000000'
            GROUP BY
                user_id
        ) c ON u.user_id = c.user_id

        LEFT OUTER JOIN

        (
        SELECT  s.user_id
                , COUNT(1) AS sales
            FROM bi_data.partnerdb_matched_transactions s
            LEFT SEMI JOIN (
                SELECT DISTINCT
                        user_id
                    FROM bi_data.partnerdb_bi_display
                    WHERE
                        partner_id IN ${hiveconf:partnerid}
                        AND partner_partition IN ${hiveconf:partnerparti}
                        AND day >= ${hiveconf:start_date}
                        AND day <= ${hiveconf:end_date}
                        AND user_id <> '00000000-0000-0000-0000-000000000000'
                ) u ON (s.user_id = u.user_id)
            WHERE
                partner_id IN ${hiveconf:partnerid}
                AND day >= ${hiveconf:start_date}
                AND day <= ${hiveconf:end_date}
            GROUP BY
                s.user_id
        ) s ON u.user_id = s.user_id
GROUP BY
    CASE
        WHEN avg_displays <= 2 THEN 'Max2'
        WHEN avg_displays <= 4 THEN 'Max4'
        WHEN avg_displays <= 6 THEN 'Max6'
        WHEN avg_displays <= 8 THEN 'Max8'
        WHEN avg_displays <= 10 THEN 'Max10'
        WHEN avg_displays <= 12 THEN 'Max12'
        WHEN avg_displays <= 14 THEN 'Max14'
        WHEN avg_displays <= 16 THEN 'Max16'
        WHEN avg_displays <= 18 THEN 'Max18'
        WHEN avg_displays <= 20 THEN 'Max20'
        WHEN avg_displays <= 22 THEN 'Max22'
        WHEN avg_displays <= 24 THEN 'Max24'
        ELSE 'Over24' END
