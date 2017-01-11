--- Environment parameters ---
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET IO.sort.mb = 1024;
SET hive.exec.reducers.max = 1500;
SET hive.cli.print.header = true;
SET hive.exec.parallel = true;


--- Query parameters ---
SET test_start_users = '2016-10-12';
SET partner_id = (4287);
SET mid_funnel_day = 12;

--- Query ---

--- Step 1 ---
DROP TABLE IF EXISTS cluo.funnel_part1;

CREATE TABLE cluo.funnel_part1 AS
SELECT
    a.user_id AS user_id
    , first_day_seen
    , max_day
    , CASE WHEN DATEDIFF(a.first_day_seen, b.max_day) < ${hiveconf:mid_funnel_day} THEN 'Lower' ELSE 'Mid' END AS funnel
FROM
(
    SELECT DISTINCT
        user_id
        , first_day_seen
    FROM
        gszigeti.cdonse_1121
    WHERE
        first_day_seen <= DATE_ADD(${hiveconf:test_start_users}, ${hiveconf:mid_funnel_day})
) a
LEFT OUTER JOIN
(
    SELECT
        user_id
        , to_date(from_unixtime(MAX(unixtime))) AS max_day
    FROM
        bi_data.partnerdb_bi_advertiser_event
    WHERE
        partner_id IN ${hiveconf:partner_id}
        AND persistent_user = TRUE
        AND day >= DATE_SUB (${hiveconf:test_start_users}, ${hiveconf:mid_funnel_day} + 1)
        AND day < ${hiveconf:test_start_users}
        AND partner_partition = 287
        AND user_id <> '00000000-0000-0000-0000-000000000000'
    GROUP BY
        user_id
) b
ON a.user_id = b.user_id
;

--- Step 2 ---
DROP TABLE IF EXISTS cluo.funnel_part2;

CREATE TABLE cluo.funnel_part2 AS
SELECT DISTINCT
    user_id
    , first_day_seen
    , 'Mid' AS funnel
FROM
    gszigeti.cdonse_1121
WHERE
    first_day_seen > DATE_ADD(${hiveconf:test_start_users}, ${hiveconf:mid_funnel_day})
;

--- Step 3 ---
DROP TABLE IF EXISTS cluo.funnel;

CREATE TABLE cluo.funnel AS
SELECT
    *
FROM
    (
    SELECT
        user_id
        , first_day_seen
        , funnel
        FROM cluo.funnel_part1
    UNION ALL
    SELECT
        user_id
        , first_day_seen
        , funnel
        FROM cluo.funnel_part2
    ) temp1
;
