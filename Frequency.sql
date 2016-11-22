-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb=512;
SET hive.enforce.bucketing = true;
SET hive.cli.print.header = true;

--- Set query parameters --
SET startdate = "2016-03-10";
SET enddate = "2016-04-18";
SET merchantid = 9627;

SELECT
    days_last_visit,
    AVG(total_displays) AS avg_total_displays,
    AVG(FCB_displays) AS avg_FCB_displays
FROM (
        SELECT
            day,
            user_id,
            CAST(days_last_visit AS INT) as days_last_visit,
            COUNT(1) as total_displays,
            SUM(is_fbx) as FCB_displays
        FROM
            bi_data.bi_display_full
        WHERE
            merchant_id = ${hiveconf:merchantid}
            AND day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
            AND user_id <> '00000000-0000-0000-0000-000000000000'
            AND host_platform = "EU"
            AND days_last_visit IS NOT NULL
        GROUP BY
            day,
            user_id,
            CAST(days_last_visit AS INT)
        ) a
GROUP BY
    days_last_visit
