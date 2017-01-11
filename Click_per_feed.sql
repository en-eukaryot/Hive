-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 1024;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

-- Set query parameters --
SET partnerid = 11075;
SET partition = pmod(11075, 1000);

-- Query --
SELECT
    cl.day
    , CASE WHEN ca.producturl LIKE '%jdn%monster%' THEN 'jdn'
    WHEN ca.producturl LIKE '%annonsoversikt%' THEN 'oversikt'
    ELSE 'other'
    END AS type
    , COUNT(1) AS click
FROM
    bi_data.partnerdb_bi_click cl
JOIN
    bi_data.partnerdb_catalogs ca
ON
    cl.item_id = ca.sqlid
    AND cl.partner_id = ca.partnerid
    AND ca.partnerid = ${hiveconf:partnerid}
    AND cl.partner_partition = ${hiveconf:partition}
    AND ca.partner_partition = ${hiveconf:partition}
WHERE
    cl.day BETWEEN '2016-11-01' AND '2016-11-30'
    AND cl.partner_id = ${hiveconf:partnerid}
GROUP BY
    cl.day
    , CASE WHEN ca.producturl LIKE '%jdn%monster%' THEN 'jdn'
    WHEN ca.producturl LIKE '%annonsoversikt%' THEN 'oversikt'
    ELSE 'other' END
