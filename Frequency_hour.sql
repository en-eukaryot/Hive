-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 1024;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET your_database = cluo;
SET startdate = "2016-11-07";
SET enddate = "2016-11-13";
SET merchantid = (3642, 3641, 5802, 3640, 3643);


USE ${hiveconf:your_database};

--- Create the table containing all data
CREATE TABLE hours_since_last_visit_raw AS
SELECT
    merchant_id,
    day,
    user_id,
    CASE WHEN days_last_visit <= 1/24 THEN 1
        WHEN days_last_visit <= 2/24 THEN 2
        WHEN days_last_visit <= 3/24 THEN 3
        WHEN days_last_visit <= 4/24 THEN 4
        WHEN days_last_visit <= 5/24 THEN 5
        WHEN days_last_visit <= 6/24 THEN 6
        WHEN days_last_visit <= 7/24 THEN 7
        WHEN days_last_visit <= 8/24 THEN 8
        WHEN days_last_visit <= 9/24 THEN 9
        WHEN days_last_visit <= 10/24 THEN 10
        WHEN days_last_visit <= 11/24 THEN 11
        WHEN days_last_visit <= 12/24 THEN 12
        WHEN days_last_visit <= 13/24 THEN 13
        WHEN days_last_visit <= 14/24 THEN 14
        WHEN days_last_visit <= 15/24 THEN 15
        WHEN days_last_visit <= 16/24 THEN 16
        WHEN days_last_visit <= 17/24 THEN 17
        WHEN days_last_visit <= 18/24 THEN 18
        WHEN days_last_visit <= 19/24 THEN 19
        WHEN days_last_visit <= 20/24 THEN 20
        WHEN days_last_visit <= 21/24 THEN 21
        WHEN days_last_visit <= 22/24 THEN 22
        WHEN days_last_visit <= 23/24 THEN 23
        WHEN days_last_visit <= 1 THEN 24 END AS hour_since_last_visit,
    COUNT(1) as total_displays,
    SUM(is_fbx) as FCB_displays
FROM
    bi_data.bi_display_full
WHERE
    merchant_id IN ${hiveconf:merchantid}
    AND day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
    AND user_id <> '00000000-0000-0000-0000-000000000000'
    AND host_platform = "EU"
    AND days_last_visit <= 1
    AND days_last_visit > 0
GROUP BY
        merchant_id,
        day,
        user_id,
        CASE WHEN days_last_visit <= 1/24 THEN 1
            WHEN days_last_visit <= 2/24 THEN 2
            WHEN days_last_visit <= 3/24 THEN 3
            WHEN days_last_visit <= 4/24 THEN 4
            WHEN days_last_visit <= 5/24 THEN 5
            WHEN days_last_visit <= 6/24 THEN 6
            WHEN days_last_visit <= 7/24 THEN 7
            WHEN days_last_visit <= 8/24 THEN 8
            WHEN days_last_visit <= 9/24 THEN 9
            WHEN days_last_visit <= 10/24 THEN 10
            WHEN days_last_visit <= 11/24 THEN 11
            WHEN days_last_visit <= 12/24 THEN 12
            WHEN days_last_visit <= 13/24 THEN 13
            WHEN days_last_visit <= 14/24 THEN 14
            WHEN days_last_visit <= 15/24 THEN 15
            WHEN days_last_visit <= 16/24 THEN 16
            WHEN days_last_visit <= 17/24 THEN 17
            WHEN days_last_visit <= 18/24 THEN 18
            WHEN days_last_visit <= 19/24 THEN 19
            WHEN days_last_visit <= 20/24 THEN 20
            WHEN days_last_visit <= 21/24 THEN 21
            WHEN days_last_visit <= 22/24 THEN 22
            WHEN days_last_visit <= 23/24 THEN 23
            WHEN days_last_visit <= 1 THEN 24 END
;

--- Do the aggregation, here by "partner id" and "hour since last vist"
--- This step can also be done in Python, R etc.
-- SELECT
--     merchant_id,
--     hour_since_last_visit,
--     AVG(total_displays) AS avg_total_displays,
--     AVG(FCB_displays) AS avg_FCB_displays
-- FROM hours_since_last_visit_raw
-- GROUP BY
--     merchant_id,
--     hour_since_last_visit
