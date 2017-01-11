-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 1024;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET your_database = cluo;
SET startdate = "2016-11-01";
SET enddate = "2016-11-30";
SET merchantid = (4155, 4154);

USE ${hiveconf:your_database};

DROP TABLE IF EXISTS frequency_raw_data;
CREATE TABLE frequency_raw_data AS
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
GROUP BY
    day,
    user_id,
    CAST(days_last_visit AS INT)
