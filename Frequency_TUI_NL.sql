-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 1024;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET startdate = "2016-11-01";
SET enddate = "2016-11-30";
SET merchantid = (4155, 4154);

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
            merchant_id IN ${hiveconf:merchantid}
            AND day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
            AND user_id <> '00000000-0000-0000-0000-000000000000'
            AND host_platform = "EU"
            AND days_last_visit IS NOT NULL
            AND is_fbx = 0
        GROUP BY
            day,
            user_id,
            CAST(days_last_visit AS INT)
        ) a
GROUP BY
    days_last_visit
