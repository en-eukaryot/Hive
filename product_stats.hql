-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 2048;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET startdate = "2016-12-01";
SET enddate = "2016-12-31";
SET enddate_conv = "2017-01-30";
SET part_id = (2429);
SET part_pt = (429);
SET your_database = cluo;

--- Query ---
USE ${hiveconf:your_database};

DROP TABLE IF EXISTS halens_product;

CREATE TABLE halens_product AS
SELECT /* +MApjoin(catalog) */
    stat.partner_id
    , stat.day
    , stat.internal_item_id
    , stat.product_name
    , catalog.value_coefficient
    , stat.displays
    , stat.clicks
    , click.revenue_euro
    , click.revenue_local
    , t.sales
FROM
    (
    SELECT
        a.partner_id
        , a.day
        , a.internal_item_id
        , a.product_name
        , SUM(a.displays) AS displays
        , SUM(a.clicks) AS clicks
    FROM
        bi_data.partnerdb_catalog_stats a
    WHERE
        a.partner_id = ${hiveconf:part_id}
        AND a.day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
    GROUP BY
        a.partner_id
        , a.day
        , a.internal_item_id
        , a.product_name
    ) stat
JOIN
    bi_datamart.bi_dim_catalog catalog
    ON stat.partner_id = catalog.merchant_id
        and stat.internal_item_id = catalog.internal_item_id
        AND catalog.merchant_id = ${hiveconf:part_id}
LEFT OUTER JOIN
    (
    SELECT
        cc.partner_id
        , cc.day
        , cc.item_id
        , SUM(revenue_local) AS revenue_local
        , SUM(revenue_euro) AS revenue_euro
    FROM
        bi_data.partnerdb_bi_click cc
    WHERE
        cc.partner_partition = ${hiveconf:part_pt}
        AND cc.partner_id = ${hiveconf:part_id}
        AND cc.day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
    GROUP BY
        cc.partner_id
        , cc.day
        , cc.item_id
    ) click
    ON stat.partner_id = click.partner_id
        and stat.day = click.day
        AND stat.internal_item_id = click.item_id
LEFT OUTER JOIN
    (
    SELECT
        b.partner_id
        , to_date(from_unixtime(b.click_timestamp)) AS click_day
        , b.click_internal_item_id
        , COUNT(DISTINCT transaction_id) AS sales
    FROM
        bi_data.partnerdb_matched_transactions b
    WHERE
        b.deduplication_matching = 1
        AND to_date(from_unixtime(b.click_timestamp)) BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
        AND b.day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate_conv}
    GROUP BY
        b.partner_id
        , to_date(from_unixtime(b.click_timestamp))
        , b.click_internal_item_id
    ) t
    ON stat.partner_id = t.partner_id
        and stat.day = t.click_day
        AND stat.internal_item_id = t.click_internal_item_id
